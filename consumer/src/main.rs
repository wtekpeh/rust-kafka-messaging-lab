use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgQueryResult};
use tokio::time::{Duration, sleep};

#[derive(Debug, Deserialize)]
struct GpsTelemetry {
    event_id: String,
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

async fn save_gps_event(
    pool: &PgPool,
    telemetry: &GpsTelemetry,
) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO gps_events (
            event_id,
            device_id,
            latitude,
            longitude,
            speed_kmh
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&telemetry.event_id)
    .bind(&telemetry.device_id)
    .bind(telemetry.latitude)
    .bind(telemetry.longitude)
    .bind(telemetry.speed_kmh)
    .execute(pool)
    .await
}

async fn save_gps_event_with_retry(
    pool: &PgPool,
    telemetry: &GpsTelemetry,
) -> Result<PgQueryResult, sqlx::Error> {
    const MAX_RETRIES: u32 = 3;

    for attempt in 0..=MAX_RETRIES {
        match save_gps_event(pool, telemetry).await {
            Ok(result) => return Ok(result),

            Err(error) => {
                if attempt == MAX_RETRIES {
                    eprintln!(
                        "PostgreSQL operation failed after {} attempts: {}",
                        MAX_RETRIES + 1,
                        error
                    );

                    return Err(error);
                }

                let delay_seconds = 2_u64.pow(attempt);

                eprintln!(
                    "PostgreSQL error: {}. Retry {}/{} in {} second(s)...",
                    error,
                    attempt + 1,
                    MAX_RETRIES,
                    delay_seconds
                );

                sleep(Duration::from_secs(delay_seconds)).await;
            }
        }
    }

    unreachable!("Retry loop must return success or error")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;

    let pool = PgPool::connect(&database_url).await?;

    println!("PostgreSQL connection pool initialized (lazy connection)");

    // 1. Configure the Kafka consumer.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "gps-json-processors-v1")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    // 2. Subscribe to our GPS topic.
    consumer.subscribe(&["orbi.gps.location"])?;

    println!("Kafka GPS consumer started. Waiting for messages...");

    // 3. Continuously receive messages.
    loop {
        match consumer.recv().await {
            Ok(message) => {
                let partition = message.partition();
                let offset = message.offset();

                let Some(payload) = message.payload() else {
                    eprintln!("Received message without a payload");
                    continue;
                };

                // 4. Deserialize JSON bytes into our Rust struct.
                match serde_json::from_slice::<GpsTelemetry>(payload) {
                    Ok(telemetry) => {
                        println!("GPS event received:");
                        println!("  Event ID: {}", telemetry.event_id);
                        println!("  Device ID: {}", telemetry.device_id);
                        println!("  Latitude: {}", telemetry.latitude);
                        println!("  Longitude: {}", telemetry.longitude);
                        println!("  Speed: {} km/h", telemetry.speed_kmh);
                        println!("  Partition: {partition}");
                        println!("  Offset: {offset}");
                        println!("-------------------------");

                        let result = save_gps_event_with_retry(&pool, &telemetry).await?;

                        if result.rows_affected() == 1 {
                            println!("GPS event inserted into PostgreSQL");
                        } else {
                            println!("Duplicate GPS event detected — insert skipped");
                        }

                        consumer.commit_message(&message, CommitMode::Sync)?;

                        println!("Kafka offset committed successfully");
                    }
                    Err(error) => {
                        eprintln!("Failed to deserialize GPS event: {error}");
                    }
                }
            }
            Err(error) => {
                eprintln!("Kafka consumer error: {error}");
            }
        }
    }
}
