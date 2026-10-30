use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct GpsTelemetry {
    event_id: String,
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Prepare GPS telemetry.
    let telemetry = GpsTelemetry {
        event_id: "GPS-RUST-001".to_string(),
        device_id: "ORBI-001".to_string(),
        latitude: 5.6037,
        longitude: -0.1870,
        speed_kmh: 42.5,
    };

    // 2. Serialize the Rust struct into JSON bytes.
    let payload = serde_json::to_vec(&telemetry)?;

    // 3. Create the Kafka producer.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("acks", "all")
        .create()?;

    // 4. Construct the Kafka record.
    let record = FutureRecord::to("orbi.gps.location")
        .key(&telemetry.device_id)
        .payload(&payload);

    // 5. Publish the record and await its delivery result.
    let delivery_result = producer.send(record, Duration::from_secs(10)).await;

    match delivery_result {
        Ok(delivery) => {
            println!("GPS event published successfully!");
            println!("Event ID: {}", telemetry.event_id);
            println!("Device ID: {}", telemetry.device_id);
            println!("Partition: {}", delivery.partition);
            println!("Offset: {}", delivery.offset);
        }
        Err((error, _message)) => {
            eprintln!("Kafka delivery failed: {error}");
            return Err(error.into());
        }
    }

    Ok(())
}
