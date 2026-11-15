use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
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

fn is_retryable_db_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_) => true,
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::PoolClosed => false,

        sqlx::Error::Database(db_error) => {
            matches!(
                db_error.code().as_deref(),
                Some("40001") | // Serialization failure
                Some("40P01") | // Deadlock detected
                Some("53300") | // Too many connections
                Some("57P03") // Cannot connect now
            )
        }

        _ => false,
    }
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
                if !is_retryable_db_error(&error) {
                    eprintln!(
                        "Non-retryable PostgreSQL error. Stopping consumer: {}",
                        error
                    );

                    return Err(error);
                }

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

async fn publish_to_dlq(
    producer: &FutureProducer,
    payload: &[u8],
    partition: i32,
    offset: i64,
    failure_reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = format!("{partition}-{offset}");

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "source_topic",
            value: Some("orbi.gps.location"),
        })
        .insert(Header {
            key: "source_partition",
            value: Some(partition.to_string().as_str()),
        })
        .insert(Header {
            key: "source_offset",
            value: Some(offset.to_string().as_str()),
        })
        .insert(Header {
            key: "failure_reason",
            value: Some(failure_reason),
        });

    let record = FutureRecord::to("orbi.gps.location.dlq")
        .key(&key)
        .payload(payload)
        .headers(headers);

    match producer.send(record, Duration::from_secs(10)).await {
        Ok(delivery) => {
            println!(
                "Message published to DLQ: partition={}, offset={}",
                delivery.partition, delivery.offset
            );
            Ok(())
        }
        Err((error, _)) => {
            eprintln!("Failed to publish message to DLQ: {error}");
            Err(error.into())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")?;

    let pool = PgPool::connect(&database_url).await?;

    println!("PostgreSQL connection pool initialized");

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

    // Producer used to publish invalid GPS messages to the DLQ.
    let dlq_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("acks", "all")
        .create()?;

    println!("Kafka DLQ producer initialized");

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

                        match consumer.commit_message(&message, CommitMode::Sync) {
                            Ok(()) => {
                                println!(
                                    "Kafka offset committed successfully: partition={}, next_offset={}",
                                    partition,
                                    offset + 1
                                );
                            }

                            Err(error) => {
                                eprintln!(
                                    "Kafka offset commit failed: partition={}, offset={}, error={}",
                                    partition, offset, error
                                );

                                return Err(error.into());
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "Failed to deserialize GPS event: partition={}, offset={}, error={}",
                            partition, offset, error
                        );

                        // Preserve the invalid message in the dead-letter queue.
                        publish_to_dlq(
                            &dlq_producer,
                            payload,
                            partition,
                            offset,
                            &error.to_string(),
                        )
                        .await?;

                        // Commit only after Kafka acknowledges DLQ delivery.
                        consumer.commit_message(&message, CommitMode::Sync)?;

                        println!(
                            "Invalid GPS event safely moved to DLQ. Original partition={}, next_offset={}",
                            partition,
                            offset + 1
                        );
                    }
                }
            }
            Err(error) => {
                eprintln!("Kafka consumer error: {error}");
            }
        }
    }
}
