# Real-Time Transaction Fraud Detection Pipeline

A simulated bank/payment platform that generates a continuous stream of transactions, detects fraud in real time using Kafka Streams, persists results to MySQL, and surfaces them on a live React dashboard.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│ Transaction Generator (Spring Boot)                      │
│ Produces synthetic transactions with Avro schema         │
└──────────────────────┬────────────────────────────────────┘
                       │ PLAINTEXT://localhost:9092
                 ┌─────▼────────┐
                 │  Apache Kafka │
                 │   (KRaft)     │
                 │               │
                 │ Topics:       │
                 │ - transactions│
                 │ - account-    │
                 │   profiles    │
                 │ - alerts      │
                 └─────┬─────────┘
                       │
                 ┌─────▼─────────────────────┐
                 │  Fraud Detection           │
                 │  Kafka Streams App (Java)  │
                 │  - velocity rule           │
                 │  - amount-anomaly rule     │
                 │  - impossible-geo rule     │
                 └─────┬──────────────────────┘
                       │ alerts topic
                 ┌─────▼──────────────────────┐
                 │  Alert Consumer API         │
                 │  (Spring Boot)              │
                 │  - @KafkaListener → MySQL   │
                 │  - REST API (paginated,     │
                 │    per-account, stats)      │
                 │  - SSE broadcast            │
                 └─────┬───────────┬────────────┘
                       │ REST      │ SSE (live push)
                       │           │
                 ┌─────▼───────────▼───────────┐
                 │  React Dashboard (Vite)      │
                 │  - Live alert feed (table)   │
                 │  - Alerts-by-reason chart    │
                 │  - Account drill-down        │
                 └───────────────────────────────┘

┌───────────────────────────────────────────────────┐
│   Confluent Schema Registry (Avro schemas)          │
│   used by every producer/consumer above             │
└───────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Streaming** | Apache Kafka (KRaft mode, `apache/kafka` image), Kafka Streams |
| **Schema** | Avro + Confluent Schema Registry |
| **Backend** | Java 17, Spring Boot 3.3, Spring for Apache Kafka, Spring Data JPA |
| **Persistence** | MySQL 8, Hibernate |
| **Real-time push** | Server-Sent Events (SSE) |
| **Frontend** | React 19, Vite, recharts (v2) |
| **Infrastructure** | Docker Compose (local dev) |
| **Future ML** | Python, scikit-learn (Phase 2, not yet built) |

---

## Project Structure

```
fraud-detection-kafka/
├── docker-compose.yml              # All infrastructure (Kafka, Schema Registry, MySQL, Kafka UI)
├── schemas/                        # Avro schema definitions (.avsc files) — shared source of truth
│   ├── Transaction.avsc
│   ├── FraudAlert.avsc
│   ├── AccountProfile.avsc
│   ├── VelocityAggregate.avsc      # internal Kafka Streams aggregation state
│   └── FraudScore.avsc             # Phase 2 placeholder, unused so far
├── transaction-generator/          # Spring Boot producer
│   └── src/main/java/com/diecocan/portfolio/fraud/
│       ├── TransactionGeneratorApplication.java
│       ├── config/KafkaTopicConfig.java
│       └── producer/TransactionProducer.java
├── fraud-streams-app/              # Kafka Streams topology
│   └── src/main/java/com/diecocan/portfolio/fraud/
│       ├── FraudStreamsApplication.java
│       ├── config/KafkaTopicConfig.java
│       └── topology/FraudDetectionTopology.java
├── alert-consumer-api/             # Consumer + REST API + SSE
│   └── src/main/java/com/diecocan/portfolio/fraud/
│       ├── AlertConsumerApiApplication.java
│       ├── entity/AlertEntity.java
│       ├── repository/AlertRepository.java
│       ├── consumer/AlertConsumer.java
│       ├── controller/AlertController.java
│       └── sse/AlertBroadcastService.java
├── dashboard/                       # React frontend (Vite)
│   └── src/
│       ├── App.jsx
│       ├── api.js
│       ├── utils.js
│       └── components/
│           ├── AlertFeed.jsx        # live SSE feed, table view
│           ├── ReasonChart.jsx      # pie chart, Suspense-based data fetching
│           └── AccountLookup.jsx    # search + sort + filter
├── ml-scoring-service/              # (Phase 2) not yet built
└── README.md
```

**Note:** `transaction-generator/` has its own `.git` (started as a separate repo, later moved into this monorepo layout). If this repo is ever git-initialized at the root, that nested `.git` will need removing first or it'll show up as an embedded/gitlink repo instead of tracked files.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Java 17+
- Maven
- Node.js + npm

### 1. Start Infrastructure

```bash
docker compose up
docker compose ps   # confirm kafka is (healthy), plus schema-registry, mysql, kafka-ui
```

### 2. Verify Connectivity

```bash
curl http://localhost:8081/subjects   # Schema Registry — expect [] on a fresh environment
```
Kafka UI: `http://localhost:8080`

### 3. Run the Backend Services (each in its own terminal)

```bash
cd transaction-generator && mvn spring-boot:run
cd fraud-streams-app && mvn spring-boot:run
cd alert-consumer-api && mvn spring-boot:run
```

`alert-consumer-api` runs on port `8082` — REST endpoints under `/api/alerts`, SSE stream at `/api/alerts/stream`.

### 4. Run the Dashboard

```bash
cd dashboard
npm install
npm run dev
```

Open `http://localhost:5173`. The Vite dev server proxies `/api/*` to `localhost:8082` (see `dashboard/vite.config.js`), so no CORS configuration is needed on the backend.

---

## Kafka Topic Design

| Topic | Key | Value Schema | Partitions | Purpose |
|-------|-----|--------------|-----------|---------|
| `transactions` | `accountId` | `Transaction.avsc` | 3 | Raw incoming transactions |
| `account-profiles` | `accountId` | `AccountProfile.avsc` | 3 | KTable: rolling per-account stats (Welford's algorithm for stddev) |
| `alerts` | `accountId` | `FraudAlert.avsc` | 3 | Rule-based fraud flags (velocity, amount anomaly, impossible geo) |
| `scores` | `transactionId` | `FraudScore.avsc` | 3 | ML model output — Phase 2, not yet produced |

---

## Design Decisions

### KRaft over ZooKeeper
Kafka's self-managed metadata quorum (KRaft) removes a whole extra service from local dev, and it's where the ecosystem has moved — no reason to learn the legacy ZooKeeper-based setup for a new project.

### `apache/kafka` (official image) over `confluentinc/cp-kafka`
The official image auto-generates and formats KRaft storage on first boot (no manual `kafka-storage.sh random-uuid` step). Schema Registry still uses Confluent's image — it only needs a Kafka bootstrap address, so it works with any Kafka distribution.

### Explicit topic provisioning, auto-create disabled
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`, with every topic declared via a Spring `NewTopic` bean. Letting Kafka silently auto-create topics with default settings (1 partition, whatever config happens to be the broker default) is exactly the kind of accidental-config problem real clusters disable this for — hit firsthand early on when a topic silently got created with 1 partition instead of the intended 3.

### SSE over WebSocket for the live dashboard feed
The feed is inherently one-directional — server pushes alerts, client never sends anything back. SSE is plain HTTP (a long-lived GET), simpler on both ends (native `EventSource` API, no client library), and auto-reconnects on its own. WebSocket/STOMP would add a message-broker layer and a client dependency for a capability (bidirectional messaging) this dashboard doesn't need.

### `recharts` v2, not v3
v3 rewrote its internals around Redux — `<Pie>` registers its config into a store and reads back computed sectors via a selector, and in testing that selector returned empty for no discoverable reason via the public API, producing a chart with a working legend but zero visible slices. Traced into the actual library source to confirm before concluding it was a library issue rather than application code. v2 uses a simpler, more directly-inspectable rendering path and is what the vast majority of existing recharts documentation and examples assume.

### Severity-based alert coloring, not just category-based
Each alert shows two independent badges: a reason badge (color-matched to the pie chart, for consistency) and a separate risk-score badge colored by severity band (critical/warning/caution). Category alone doesn't communicate urgency — a 52%-risk `AMOUNT_ANOMALY` and a 98%-risk one are very different in practice, and the UI should say so at a glance.

### Backward-compatible schema evolution
Schema Registry is configured for `BACKWARD` compatibility (the default), meaning new optional fields can be added to any schema over time without breaking producers/consumers already running old code — demonstrated directly when `AccountProfile.avsc` gained geography-tracking fields (`lastCity`, `lastCountry`, `lastTransactionTimestamp`) mid-project via the standard `["null", type]` + `"default": null` pattern.

---

## Troubleshooting

### "Connection to node -1 could not be established"
Check `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`:
- Host machine → use `localhost:9092`
- Container-to-container → use `kafka:9093`

### Schema Registry returns errors
Confirm it's pointed at the right Kafka listener in `docker-compose.yml`:
```
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9093
```

### Topics don't appear in Kafka UI
Refresh the browser — Kafka UI may not update instantly.

### MySQL connection fails
Check password in `docker-compose.yml` matches your connection string. Default is `root:root`.

### `Error while fetching metadata ... UNKNOWN_TOPIC_OR_PARTITION`
`KAFKA_AUTO_CREATE_TOPICS_ENABLE` is set to `false`, so topics only exist if explicitly created via a `NewTopic` bean. Check:
1. The class holding the `NewTopic` bean has `@Configuration` on it.
2. The topic name in `TopicBuilder.name(...)` matches what the producer sends to.
3. `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`

### `Web server failed to start. Port 8080 was already in use.`
Kafka UI already uses host port 8080. If a service has no REST endpoints, disable its embedded web server instead of picking a different port:
```yaml
spring:
  main:
    web-application-type: none
```

### Dashboard shows stale state after a dependency change (e.g. a library version swap)
Vite's dev-dependency pre-bundling cache (`node_modules/.vite`) can serve stale code after `npm install`ing a different package version. If behavior doesn't match what the source code says it should, try a hard refresh (clear cache) in the browser before assuming the code is wrong.

### Clearing test data between iterations
`scripts/empty-topic.sh <topic-name>` deletes a single topic (recreated empty by its owning app's `NewTopic` bean on next start). `scripts/empty-topic.sh --all` deletes every application topic with a confirmation prompt, automatically excluding Kafka/Schema-Registry internals and Kafka Streams-managed changelog/repartition topics (which need the dedicated `kafka-streams-application-reset.sh` tool instead, not a plain topic delete).

---

## Resources

- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Kafka Streams Topology](https://kafka.apache.org/documentation/#streams_topology)
- [Avro Specification](https://avro.apache.org/docs/current/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
