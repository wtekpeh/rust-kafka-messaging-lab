use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

use rdkafka::error::KafkaError;

#[derive(Debug)]
enum TransactionRecovery {
    Retry,
    Abort,
    Fatal,
    Unknown,
}

fn classify_transaction_error(error: &KafkaError) -> TransactionRecovery {
    match error {
        KafkaError::Transaction(inner) => {
            if inner.is_fatal() {
                TransactionRecovery::Fatal
            } else if inner.txn_requires_abort() {
                TransactionRecovery::Abort
            } else if inner.is_retriable() {
                TransactionRecovery::Retry
            } else {
                TransactionRecovery::Unknown
            }
        }

        KafkaError::MessageProduction(code) => {
            eprintln!("Kafka message production error code: {code:?}");

            TransactionRecovery::Unknown
        }

        _ => TransactionRecovery::Unknown,
    }
}

fn abort_kafka_transaction(producer: &FutureProducer) -> Result<(), rdkafka::error::KafkaError> {
    println!("Attempting to abort Kafka transaction...");

    producer.abort_transaction(rdkafka::util::Timeout::After(Duration::from_secs(10)))?;

    println!("Kafka transaction aborted successfully");

    Ok(())
}

fn inspect_kafka_error(error: &KafkaError) {
    eprintln!("Kafka error: {error}");

    match error {
        KafkaError::Transaction(rdkafka_error) => {
            eprintln!("Retriable: {}", rdkafka_error.is_retriable());
            eprintln!(
                "Transaction requires abort: {}",
                rdkafka_error.txn_requires_abort()
            );
            eprintln!("Fatal: {}", rdkafka_error.is_fatal());
        }

        _ => {
            eprintln!("This error is not a transaction-specific Kafka error.");
        }
    }
}

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
        "event_id": "GPS-TX-002",
        "device_id": "ORBI-001",
        "latitude": 5.6037,
        "longitude": -0.187,
        "speed_kmh": 42.5
    }"#;

    let audit_event = r#"{
        "event_id": "GPS-TX-002",
        "action": "GPS_ENRICHED",
        "status": "SUCCESS"
    }"#;

    let gps_record = FutureRecord::to("gps.enriched")
        .key("ORBI-001")
        .payload(gps_event);

    match producer.send(gps_record, Duration::from_secs(10)).await {
        Ok(delivery) => {
            println!(
                "Enriched GPS event delivered: partition={}, offset={}",
                delivery.partition, delivery.offset
            );
        }

        Err((error, _)) => {
            eprintln!("Failed to publish enriched GPS event: {error}");

            inspect_kafka_error(&error);

            let recovery = classify_transaction_error(&error);

            eprintln!("Error classification: {recovery:?}");

            match recovery {
                TransactionRecovery::Fatal => {
                    eprintln!("Fatal Kafka error. Producer must not be reused.");
                }

                TransactionRecovery::Abort => {
                    abort_kafka_transaction(&producer)?;
                }

                TransactionRecovery::Retry | TransactionRecovery::Unknown => {
                    eprintln!(
                        "GPS publication failed. Attempting to abort the current transaction."
                    );

                    abort_kafka_transaction(&producer)?;
                }
            }

            return Err(error.into());
        }
    }

    let audit_record = FutureRecord::to("gps.audit")
        .key("GPS-TX-002")
        .payload(audit_event);

    match producer.send(audit_record, Duration::from_secs(10)).await {
        Ok(delivery) => {
            println!(
                "Audit event delivered: partition={}, offset={}",
                delivery.partition, delivery.offset
            );
        }

        Err((error, _)) => {
            eprintln!("Failed to publish audit event: {error}");

            inspect_kafka_error(&error);

            let recovery = classify_transaction_error(&error);

            eprintln!("Error classification: {recovery:?}");

            match recovery {
                TransactionRecovery::Fatal => {
                    eprintln!("Fatal Kafka error. Producer must not be reused.");
                }

                TransactionRecovery::Abort => {
                    abort_kafka_transaction(&producer)?;
                }

                TransactionRecovery::Retry | TransactionRecovery::Unknown => {
                    eprintln!("Audit publication failed. Attempting to abort the transaction.");

                    abort_kafka_transaction(&producer)?;
                }
            }

            return Err(error.into());
        }
    }

    let max_commit_attempts = 3;

    for attempt in 1..=max_commit_attempts {
        println!("Committing Kafka transaction: attempt {attempt}/{max_commit_attempts}");

        match producer.commit_transaction(rdkafka::util::Timeout::After(Duration::from_secs(10))) {
            Ok(()) => {
                println!("Kafka transaction committed successfully");
                break;
            }

            Err(error) => {
                inspect_kafka_error(&error);

                let recovery = classify_transaction_error(&error);

                match recovery {
                    TransactionRecovery::Retry if attempt < max_commit_attempts => {
                        eprintln!("Retriable commit error. Retrying the commit operation...");

                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }

                    TransactionRecovery::Abort => {
                        eprintln!("Kafka requires this transaction to be aborted.");

                        abort_kafka_transaction(&producer)?;

                        return Err(error.into());
                    }

                    _ => {
                        eprintln!("Commit unsuccessful. Recovery classification: {recovery:?}");

                        return Err(error.into());
                    }
                }
            }
        }
    }

    Ok(())
}
