# rust-kafka-messaging-lab

Hands-on Apache Kafka distributed messaging lab using Rust, Tokio, and rdkafka.

This repository is a progressive learning lab for Kafka's distributed messaging model. The goal is to understand how producers, partitions, consumer groups, and offset management work by implementing and testing them in Rust.

> **Status:** Milestone 1 complete — Rust producer, Rust consumer, and manual offset commits. The lab is still in development; it is not a production-ready messaging service.

## Technology stack

- **Rust** with **Tokio** for asynchronous applications
- **rdkafka 0.39** (bindings to librdkafka) for Kafka clients
- **Serde / serde_json** for telemetry serialization and deserialization
- **Apache Kafka 4.1.1** in single-node KRaft mode via Docker Compose
- Cargo workspace with separate `producer` and `consumer` crates

## Repository layout

```text
rust-kafka-messaging-lab/
├── Cargo.toml
├── Cargo.lock
├── docker-compose.yml
├── producer/
│   ├── Cargo.toml
│   └── src/main.rs
├── consumer/
│   ├── Cargo.toml
│   └── src/main.rs
└── README.md
```

## Kafka lab configuration

| Setting | Value |
| --- | --- |
| Bootstrap server (from host) | `localhost:9092` |
| Topic | `orbi.gps.location` |
| Partitions | `3` |
| Replication factor | `1` |
| JSON consumer group | `gps-json-processors-v1` |
| Message key | `device_id` (e.g. `ORBI-001`) |

The `orbi.gps.location` name and device identifiers are sample telemetry data for this standalone learning lab.

### Start Kafka

```bash
docker compose up -d
docker logs kafka-lab --tail 30
```

Create the topic if it does not already exist:

```bash
docker exec kafka-lab \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic orbi.gps.location \
  --partitions 3 \
  --replication-factor 1
```

Inspect its partitions:

```bash
docker exec kafka-lab \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orbi.gps.location
```

> A replication factor of 1 is suitable for this local experiment, but does **not** provide broker-failure tolerance.

## Milestone 1 — Rust producer and consumer

### 1. Producer: publish keyed JSON telemetry

The `producer` crate uses `FutureProducer` and `FutureRecord` to:

- Serialize a GPS event to JSON.
- Set the Kafka record key to `device_id`.
- Publish to `orbi.gps.location` with `acks=all`.
- Await the delivery result and print the assigned partition and offset.

Run:

```bash
cargo run -p producer
```

Example event:

```json
{
  "event_id": "GPS-RUST-001",
  "device_id": "ORBI-001",
  "latitude": 5.6037,
  "longitude": -0.187,
  "speed_kmh": 42.5
}
```

**Verified:** The Rust producer published `GPS-RUST-001` with key `ORBI-001` to **partition 1, offset 5**. A Kafka console consumer independently read back that exact JSON record.

A message key determines partition routing. Records with the same key are normally routed to the same partition while the topic's partition count and partitioner configuration remain unchanged. Kafka provides ordering **within** a partition, not across the whole topic.

### 2. Consumer: deserialize GPS telemetry

The `consumer` crate uses `StreamConsumer` to:

- Join a named consumer group.
- Subscribe to the GPS topic.
- Receive messages asynchronously.
- Deserialize JSON into a Rust `GpsTelemetry` struct.
- Display the partition and offset.
- Report malformed JSON without treating it as a successfully processed event.

Run in another terminal:

```bash
cargo run -p consumer
```

Earlier console tests placed comma-separated text records in the topic. The JSON consumer reported deserialization errors for those records as expected, then successfully decoded the Rust-produced JSON event at **partition 1, offset 5**.

### 3. Manual offset commits

The current JSON consumer is configured with:

```rust
.set("group.id", "gps-json-processors-v1")
.set("auto.offset.reset", "latest")
.set("enable.auto.commit", "false")
.set("enable.auto.offset.store", "false")
```

After successfully decoding and processing a message, it explicitly commits its position:

```rust
consumer.commit_message(&message, CommitMode::Sync)?;
```

**A committed offset is the next position to read**, not the offset of the record just processed. Kafka stores consumer-group progress in its internal `__consumer_offsets` topic, indexed by consumer group, topic, and partition.

**Verified test:**

1. The consumer processed `GPS-RUST-001` at **partition 1, offset 6**.
2. It printed `Kafka offset committed successfully`.
3. The consumer stopped.
4. Kafka's group-inspection command showed **CURRENT-OFFSET 7**, **LOG-END-OFFSET 7**, and **LAG 0** for partition 1.
5. The same consumer group was restarted and waited for new messages instead of immediately replaying the processed record.

Inspect the committed position:

```bash
docker exec kafka-lab \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group gps-json-processors-v1
```

Observed output (relevant columns):

```text
GROUP                   TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
gps-json-processors-v1  orbi.gps.location  1          7               7               0
```

The reported `CURRENT-OFFSET` belongs to the **consumer group**, not to a specific running Rust process. This is why another instance of the same group can resume from a previously committed position.

**Important semantics:** Committing an offset does not delete the record. Kafka retains records according to its retention policy. A different consumer group can maintain an independent position and read the same records.

The current implementation demonstrates the basis for **at-least-once** processing. It does not yet guarantee exactly-once application-side effects. If processing succeeds but the commit fails, a record may be processed again. Application-level idempotency will be addressed in a later milestone.

## Concepts demonstrated

| Concept | What we observed |
| --- | --- |
| Topic and partitions | A topic with three independently ordered partitions |
| Unkeyed messages | Console-produced records can be batched into one partition |
| Keyed messages | `ORBI-001` records routed consistently to partition 1 during our tests |
| Producer delivery | Rust received partition and offset from Kafka after publishing |
| Consumer groups | Progress tracked by `group.id` |
| Offset reset | `earliest`/`latest` affect startup when no valid committed offset exists |
| Manual commits | Processing offset 6 resulted in committed offset 7 |
| Restart recovery | Group progress remained available after the consumer exited |
| Serialization | Rust JSON records decoded; earlier CSV-style records rejected |

## Kafka vs. NATS JetStream — learning comparison

| NATS JetStream | Kafka |
| --- | --- |
| Consumer acknowledges individual messages | Consumer commits its next offset per topic partition |
| Stream stores messages subject to retention | Partitioned log stores records subject to retention |
| Queue-style workers distribute deliveries | Consumer-group members receive partition assignments |
| Redelivery depends on acknowledgment state | Replay depends on committed offsets and consumer position |

These are conceptual comparisons, not exact feature equivalences.

## Current limitations

- Only one Kafka broker and no replication-based fault tolerance.
- The producer currently emits a fixed sample event; rerunning it creates another record even with the same `event_id`.
- The consumer prints processed data rather than persisting it to a database.
- Malformed records are logged but no dead-letter or recovery policy has been implemented.
- Manual commits occur after successful JSON processing, but durable side effects and idempotency are not yet implemented.
- Multi-consumer partition assignments, rebalancing, and crash recovery have not yet been tested.

## Planned milestones

1. Run multiple Rust consumers in one group and inspect partition assignment.
2. Observe rebalancing when a consumer joins, exits, or fails.
3. Explore offset recovery and at-least-once processing under failure.
4. Add PostgreSQL persistence with idempotent writes.
5. Introduce malformed-record handling, retries, and dead-letter topics.
6. Study delivery guarantees, replication, and production reliability settings.
7. Containerize Rust clients and explore Kafka workloads on Kubernetes.

## Build checks

```bash
cargo fmt --all
cargo check --workspace
```

## Learning approach

This repository documents experiments incrementally. Each milestone records what was **actually tested**, what the result means, and what remains to be implemented.
