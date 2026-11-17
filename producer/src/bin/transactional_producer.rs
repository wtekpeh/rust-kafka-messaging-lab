use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("transactional.id", "gps-transaction-lab-001")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .create()?;

    println!("Transactional Kafka producer configured");

    println!("Initializing Kafka transactions...");

    producer.init_transactions(rdkafka::util::Timeout::After(
        std::time::Duration::from_secs(10),
    ))?;

    println!("Kafka transactions initialized successfully");

    producer.begin_transaction()?;

    println!("Kafka transaction started");

    let gps_event = r#"{
        "event_id": "GPS-TX-001",
        "device_id": "ORBI-001",
        "latitude": 5.6037,
        "longitude": -0.187,
        "speed_kmh": 42.5
    }"#;

    let audit_event = r#"{
        "event_id": "GPS-TX-001",
        "action": "GPS_ENRICHED",
        "status": "SUCCESS"
    }"#;

    let gps_record = FutureRecord::to("gps.enriched")
        .key("ORBI-001")
        .payload(gps_event);

    producer
        .send(gps_record, Duration::from_secs(10))
        .await
        .map_err(|(error, _)| error)?;

    println!("Enriched GPS event delivered");

    let audit_record = FutureRecord::to("gps.audit")
        .key("GPS-TX-001")
        .payload(audit_event);

    producer
        .send(audit_record, Duration::from_secs(10))
        .await
        .map_err(|(error, _)| error)?;

    println!("Audit event delivered");

    producer.commit_transaction(rdkafka::util::Timeout::After(Duration::from_secs(10)))?;

    println!("Kafka transaction committed successfully");

    Ok(())
}
