# Real-Time ML Feature Engineering Pipeline
## Apache Kafka + Apache Flink — Production-Style Streaming Pipeline

A complete, containerised real-time feature engineering system that computes ML features from live user interaction streams with sub-minute latency.

---

## Quick Start

```bash
# 1. Clone and enter the project
cd Real-Time-ML-Feature-Engineering-Pipeline-with-Kafka-and-Apache-Flink

# 2. Start the entire pipeline (one command)
docker-compose up --build -d

# 3. Watch services become healthy (~3–5 minutes)
docker-compose ps

# 4. Open the dashboard
open http://localhost:8080

# 5. Inspect the Flink Web UI
open http://localhost:8081
```

---

## Architecture

```
Producer (Python)
   │  user-events (3 partitions)
   │  content-metadata (compacted)
   ▼
Apache Kafka (cp-kafka:7.5.3)
   │
   ├──► Apache Flink Job (PyFlink 1.17.2)
   │         EventTime + 30-second BoundedOutOfOrderness Watermark
   │         ┌─────────────────────────────────────────────────────┐
   │         │  click_rate       → TUMBLE(1 hour)                 │
   │         │  avg_dwell_time   → TUMBLE(1 hour)                 │
   │         │  engagement_rate  → HOP(15 min window / 5 min slide)│
   │         │  category_affinity→ JOIN content_metadata + TUMBLE  │
   │         └─────────────────────────────────────────────────────┘
   │               │
   │               ▼  feature-store (compacted, key=entity_id:feature_name)
   │
   └──► Dashboard (FastAPI + WebSocket)
             http://localhost:8080
```

---

## Services

| Service | Container | Port | Role |
|---|---|---|---|
| Zookeeper | `zookeeper` | 2181 | Kafka coordination |
| Kafka | `kafka` | 9092 / 29092 | Event bus & feature store backend |
| kafka-init | `kafka-init` | — | Creates topics with correct configs |
| Flink Job Manager | `flink-jobmanager` | **8081** | Cluster coordinator + Web UI |
| Flink Task Manager | `flink-taskmanager` | — | Operator execution |
| Flink Job Submit | `flink-job-submit` | — | Submits PyFlink job |
| Producer | `producer` | — | Simulates user events |
| Dashboard | `dashboard` | **8080** | Observability UI |

---

## Kafka Topics

| Topic | Partitions | Policy | Schema |
|---|---|---|---|
| `user-events` | 3 | delete | `{user_id, content_id, event_type, dwell_time_ms, timestamp}` |
| `content-metadata` | 1 | **compact** | `{content_id, category, creator_id, publish_timestamp}` |
| `feature-store` | 3 | **compact** | `{entity_id, feature_name, feature_value, computed_at}` |

---

## Computed Features

| Feature | Entity | Window | Method |
|---|---|---|---|
| `click_rate` | user | Tumbling 1 hr | `TUMBLE(..., INTERVAL '1' HOUR)` |
| `avg_dwell_time` | user | Tumbling 1 hr | `TUMBLE(..., INTERVAL '1' HOUR)` |
| `engagement_rate` | content | Sliding 15 min / 5 min | `HOP(..., INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)` |
| `category_affinity_<cat>` | user | Tumbling 1 hr | `JOIN content_metadata` + `TUMBLE(1 hr)` |

**Watermark:** `BoundedOutOfOrderness = 30 seconds`  
**Late events:** Producer intentionally generates 5% of events 35–90 simulated seconds late to demonstrate watermark-based filtering.

---

## Dashboard

Open **http://localhost:8080** after startup.

- **Entity Viewer** — enter `user_001` or `content_001` to see all live features
- **Live Feature Stream** — real-time WebSocket feed of feature updates
- **Feature Freshness** — staleness bars for click_rate, avg_dwell_time, engagement_rate
- **Watermark Lag** — estimated pipeline lag vs wall clock
- **Late Events Counter** — events dropped due to watermark

---

## Batch Comparison Script

After the pipeline has been running for at least 2 minutes:

```bash
pip install confluent-kafka pandas
python scripts/batch_comparison.py
```

Reads all raw events from Kafka, recomputes features in Pandas, and prints a side-by-side comparison with the streaming values. See `ANALYSIS.md` for findings.

---

## Test Entity IDs (for Evaluator)

```json
{
  "test_user_id":    "user_001",
  "test_content_id": "content_001"
}
```

`user_001` is a **binge-watcher** archetype — they heavily interact with sci-fi content (`content_001`–`content_004`), producing rich feature values within the first simulated hour.

---

## Useful Commands

```bash
# Check all service health
docker-compose ps

# Follow producer logs
docker-compose logs -f producer

# Follow Flink job logs
docker-compose logs -f flink-job-submit

# List topics and verify cleanup.policy
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic feature-store

# Sample feature-store messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic feature-store \
  --from-beginning \
  --max-messages 20

# Stop everything
docker-compose down -v
```

---

## Report

See [`ANALYSIS.md`](ANALYSIS.md) for:
- **Batch vs. Streaming Divergence** — comparison of feature values, window alignment, and late-event impact
- **Late Event Handling** — evidence of watermark filtering with concrete metrics and log examples
