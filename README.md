# rust-kafka-messaging-lab

Hands-on Apache Kafka distributed messaging lab using Rust, Tokio, and rdkafka.

This repository is a progressive learning lab for Kafka's distributed messaging model. The goal is to understand how producers, partitions, consumer groups, and offset management work by implementing and testing them in Rust.

> **Status:** Milestones 1–3 complete — Rust producer and consumer, consumer-group rebalancing, manual offset commits, PostgreSQL persistence, idempotent processing, and crash recovery. This is a learning lab, not a production-ready messaging service.

## Technology stack

- **Rust** with **Tokio** for asynchronous applications
- **rdkafka 0.39** (bindings to librdkafka) for Kafka clients
- **Serde / serde_json** for telemetry serialization and deserialization
- **SQLx** for asynchronous PostgreSQL persistence
- **PostgreSQL** for durable GPS event storage and idempotency
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
- Persist valid GPS events to PostgreSQL using SQLx.
- Prevent duplicate database inserts using a unique event ID.
- Commit Kafka offsets only after successful database processing.
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

After successfully deserializing a message and completing its PostgreSQL insert or duplicate check, the consumer explicitly commits its position:

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

The consumer implements **at-least-once processing with idempotent PostgreSQL persistence**. If PostgreSQL successfully stores an event but the consumer crashes before committing its Kafka offset, Kafka can redeliver the event.

The database's unique `event_id` constraint prevents duplicate storage. This behavior was verified through a simulated consumer crash in Milestone 3.

This does not guarantee exactly-once behavior for arbitrary external side effects.

## Milestone 2 — Consumer groups and partition rebalancing

We investigated Kafka consumer-group behavior by running multiple Rust consumer instances against the three-partition `orbi.gps.location` topic.

All instances used the same consumer group:

```text
gps-json-processors-v1
```

### Consumer assignment experiments

| Consumers | Observed behavior |
| --- | --- |
| 1 | One consumer handled all three partitions |
| 2 | Kafka distributed the partitions between two consumers |
| 3 | Each consumer received one partition |
| 4 | Three consumers received partitions; one remained idle |

Kafka assigns each partition to at most one active consumer within a consumer group at a time.

For a three-partition topic, adding more than three consumers to the same group does not increase partition-level processing parallelism.

### Rebalancing and consumer failure

We stopped an active consumer while four instances were participating in the same group.

**Verified results:**

1. Kafka detected the consumer's departure.
2. Partition ownership was redistributed.
3. The previously idle consumer received partition 2.
4. A GPS event published to partition 2 was processed by the replacement consumer.
5. The consumer committed the event's offset successfully.

This demonstrated automatic partition reassignment and recovery through consumer-group coordination.

Inspect consumer assignments:

```bash
docker exec kafka-lab \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group gps-json-processors-v1 \
  --members --verbose
```

## Milestone 3 — PostgreSQL persistence, idempotency, and crash recovery

The Rust Kafka consumer now persists GPS telemetry to PostgreSQL using SQLx.

The objective is to ensure that a successfully stored event is not inserted again when Kafka redelivers it.

### PostgreSQL setup

This experiment reused an existing local PostgreSQL container named `dev_postgres`, accessible from the host on port `5432`.

PostgreSQL is currently managed separately from this repository's Docker Compose configuration.

A dedicated database was created:

```sql
CREATE DATABASE kafka_lab;
```

Inside `kafka_lab`, the following table was created:

```sql
CREATE TABLE gps_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    device_id TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmh DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The unique constraint on `event_id` ensures that each logical GPS event can be stored only once.

### Database connection

The Rust consumer reads its PostgreSQL connection string from the `DATABASE_URL` environment variable.

Example connection format:

```text
postgres://postgres:<password>@localhost:5432/kafka_lab
```

Configure `DATABASE_URL` with your own database credentials before starting the consumer.

Do not commit database passwords or credential-bearing `.env` files.

The consumer establishes a connection pool using SQLx:

```rust
let database_url = std::env::var("DATABASE_URL")?;
let pool = PgPool::connect(&database_url).await?;
```

### Message processing sequence

The consumer follows this sequence:

1. Receive a Kafka message.
2. Deserialize the JSON payload into `GpsTelemetry`.
3. Execute a PostgreSQL insert.
4. Detect whether the event was inserted or already existed.
5. Commit the Kafka offset only after the database operation succeeds.

The SQL insert uses:

```sql
INSERT INTO gps_events (
    event_id,
    device_id,
    latitude,
    longitude,
    speed_kmh
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING;
```

SQLx's `rows_affected()` returns `1` when a new row is inserted and `0` when a duplicate is skipped.

After successful database processing, the consumer commits its Kafka offset:

```rust
consumer.commit_message(&message, CommitMode::Sync)?;
```

### Experiment 1 — Duplicate prevention

We published the same logical GPS event, `GPS-DB-001`, twice.

| Partition | Offset | PostgreSQL result |
| --- | --- | --- |
| 0 | 1 | Event inserted |
| 0 | 2 | Duplicate detected; insert skipped |

Both Kafka records were processed and their offsets committed.

PostgreSQL verification:

```sql
SELECT event_id, COUNT(*)
FROM gps_events
WHERE event_id = 'GPS-DB-001'
GROUP BY event_id;
```

**Observed result:** One stored record.

### Experiment 2 — Crash before offset commit

We introduced a temporary failure condition that terminated the Rust process after the PostgreSQL insert but before the Kafka offset commit.

The test event was:

```text
GPS-CRASH-001
```

Kafka delivered the event at partition 0, offset 3.

PostgreSQL successfully stored the event, but the consumer terminated before committing its progress.

Kafka consumer-group inspection showed:

| Metric | Before recovery |
| --- | --- |
| Partition | 0 |
| Committed offset | 3 |
| Log end offset | 4 |
| Consumer lag | 1 |

The database already contained `GPS-CRASH-001`, even though Kafka still considered the message uncommitted.

### Experiment 3 — Recovery after restart

We removed the temporary crash condition and restarted the Rust consumer.

Kafka redelivered `GPS-CRASH-001` at partition 0, offset 3.

The consumer output confirmed:

```text
Duplicate GPS event detected — insert skipped
Kafka offset committed successfully
```

Final Kafka inspection showed:

| Partition | Current offset | Log end offset | Lag |
| --- | --- | --- | --- |
| 0 | 4 | 4 | 0 |
| 1 | 8 | 8 | 0 |
| 2 | 2 | 2 | 0 |

PostgreSQL verification confirmed that `GPS-CRASH-001` still existed exactly once.

### Key learning outcome

Kafka offset commits and PostgreSQL transactions are separate operations.

A consumer can successfully write to PostgreSQL and crash before recording its progress in Kafka.

By combining:

- Manual offset commits after successful database processing
- A unique database constraint on `event_id`
- `INSERT ... ON CONFLICT DO NOTHING`

the consumer can safely recover from this failure without inserting duplicate GPS events.

This demonstrates **at-least-once processing with idempotent database writes**, not universal exactly-once execution of arbitrary side effects.

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
| Consumer scaling | Three partitions supported three active partition owners |
| Idle consumers | A fourth consumer had no partition assignment |
| Rebalancing | Partition ownership changed when a consumer stopped |
| PostgreSQL persistence | SQLx successfully inserted GPS telemetry |
| Idempotency | Duplicate `event_id` values did not create duplicate rows |
| Crash recovery | Kafka redelivered an uncommitted event after restart |
| At-least-once processing | PostgreSQL insert succeeded before a crash; replay was handled safely |

## Kafka vs. NATS JetStream — learning comparison

| NATS JetStream | Kafka |
| --- | --- |
| Consumer acknowledges individual messages | Consumer commits its next offset per topic partition |
| Stream stores messages subject to retention | Partitioned log stores records subject to retention |
| Queue-style workers distribute deliveries | Consumer-group members receive partition assignments |
| Redelivery depends on acknowledgment state | Replay depends on committed offsets and consumer position |

These are conceptual comparisons, not exact feature equivalences.

## Current limitations

- Kafka runs as a single broker with replication factor 1, without broker-failure tolerance.
- The producer emits a fixed sample event rather than generating unique event identifiers automatically.
- PostgreSQL must be configured separately; it is not included in this repository's Docker Compose setup.
- Database failures currently terminate the consumer rather than being handled through a retry strategy.
- Malformed JSON is logged without a dead-letter topic or explicit poison-message policy.
- Idempotency is enforced for PostgreSQL inserts through `event_id` uniqueness; exactly-once behavior across arbitrary external side effects is not guaranteed.
- Consumer observability, graceful shutdown, and production deployment remain future work.

## Planned milestones

1. Implement consumer error handling and retry strategies for temporary PostgreSQL failures.
2. Introduce dead-letter topics and policies for malformed or permanently failing records.
3. Explore Kafka producer idempotence, transactions, delivery guarantees, and replication.
4. Improve consumer lifecycle handling, graceful shutdown, and observability.
5. Containerize the Rust producer and consumer.
6. Deploy Kafka-connected Rust workloads on Kubernetes and study scaling and recovery.

## Build checks

```bash
cargo fmt --all
cargo check --workspace
```

## Learning approach

This repository documents experiments incrementally. Each milestone records what was **actually tested**, what the result means, and what remains to be implemented.
