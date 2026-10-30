use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct GpsTelemetry {
    event_id: String,
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
