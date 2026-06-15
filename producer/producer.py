"""
Data Producer – Real-Time ML Feature Engineering Pipeline
=========================================================
Simulates realistic user interaction events and publishes them to Kafka.

Key behaviours
--------------
* Accelerated time: TIME_ACCELERATION_FACTOR real seconds = 1 simulated minute.
  (default 60 → 1 real second ≈ 1 simulated minute)
* Late events: LATE_EVENT_PERCENTAGE of messages carry a timestamp that is
  LATE_EVENT_MIN_DELAY_S … LATE_EVENT_MAX_DELAY_S *simulated* seconds in
  the past – outside Flink's 30-second watermark tolerance, so they are
  dropped / counted as late by the pipeline.
* Content metadata is published once at startup to the compacted topic.

Environment variables (all have defaults)
------------------------------------------
KAFKA_BOOTSTRAP_SERVERS   – e.g. kafka:9092
USER_EVENTS_TOPIC         – user-events
CONTENT_METADATA_TOPIC    – content-metadata
EVENTS_PER_SECOND         – 10  (real-time rate)
TIME_ACCELERATION_FACTOR  – 60  (1 real second = 60 simulated seconds)
LATE_EVENT_PERCENTAGE     – 0.05
LATE_EVENT_MIN_DELAY_S    – 35  (simulated seconds)
LATE_EVENT_MAX_DELAY_S    – 90  (simulated seconds)
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from confluent_kafka import Producer, KafkaException

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Producer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092")
USER_EVENTS_TOPIC  = os.environ.get("USER_EVENTS_TOPIC",        "user-events")
METADATA_TOPIC     = os.environ.get("CONTENT_METADATA_TOPIC",   "content-metadata")
EVENTS_PER_SECOND  = int(os.environ.get("EVENTS_PER_SECOND",    "10"))
TIME_ACCEL         = int(os.environ.get("TIME_ACCELERATION_FACTOR", "60"))
LATE_PCT           = float(os.environ.get("LATE_EVENT_PERCENTAGE",  "0.05"))
LATE_MIN_S         = int(os.environ.get("LATE_EVENT_MIN_DELAY_S",   "35"))
LATE_MAX_S         = int(os.environ.get("LATE_EVENT_MAX_DELAY_S",   "90"))

# ---------------------------------------------------------------------------
# Static Content Catalogue  (20 items × 5 categories)
# ---------------------------------------------------------------------------
CONTENT_CATALOGUE: list[dict[str, str]] = [
    # sci-fi
    {"content_id": "content_001", "category": "sci-fi",      "creator_id": "creator_01"},
    {"content_id": "content_002", "category": "sci-fi",      "creator_id": "creator_02"},
    {"content_id": "content_003", "category": "sci-fi",      "creator_id": "creator_03"},
    {"content_id": "content_004", "category": "sci-fi",      "creator_id": "creator_04"},
    # drama
    {"content_id": "content_005", "category": "drama",       "creator_id": "creator_05"},
    {"content_id": "content_006", "category": "drama",       "creator_id": "creator_06"},
    {"content_id": "content_007", "category": "drama",       "creator_id": "creator_07"},
    {"content_id": "content_008", "category": "drama",       "creator_id": "creator_08"},
    # comedy
    {"content_id": "content_009", "category": "comedy",      "creator_id": "creator_09"},
    {"content_id": "content_010", "category": "comedy",      "creator_id": "creator_10"},
    {"content_id": "content_011", "category": "comedy",      "creator_id": "creator_11"},
    {"content_id": "content_012", "category": "comedy",      "creator_id": "creator_12"},
    # news
    {"content_id": "content_013", "category": "news",        "creator_id": "creator_13"},
    {"content_id": "content_014", "category": "news",        "creator_id": "creator_14"},
    {"content_id": "content_015", "category": "news",        "creator_id": "creator_15"},
    {"content_id": "content_016", "category": "news",        "creator_id": "creator_16"},
    # sports
    {"content_id": "content_017", "category": "sports",      "creator_id": "creator_17"},
    {"content_id": "content_018", "category": "sports",      "creator_id": "creator_18"},
    # documentary
    {"content_id": "content_019", "category": "documentary", "creator_id": "creator_19"},
    {"content_id": "content_020", "category": "documentary", "creator_id": "creator_20"},
]

CONTENT_IDS = [c["content_id"] for c in CONTENT_CATALOGUE]

# ---------------------------------------------------------------------------
# User Archetypes  (100 users, 4 archetypes × 25 users each)
# ---------------------------------------------------------------------------
ARCHETYPES = {
    "binge-watcher":  {
        "preferred_categories": ["sci-fi", "drama"],
        "event_weights": {"view": 0.30, "click": 0.25, "like": 0.20, "share": 0.10, "skip": 0.15},
        "dwell_mean_ms": 120_000,
        "dwell_std_ms":   40_000,
    },
    "news-scanner": {
        "preferred_categories": ["news", "documentary"],
        "event_weights": {"view": 0.45, "click": 0.30, "like": 0.05, "share": 0.05, "skip": 0.15},
        "dwell_mean_ms": 25_000,
        "dwell_std_ms":  10_000,
    },
    "casual-browser": {
        "preferred_categories": ["comedy", "sci-fi", "sports"],
        "event_weights": {"view": 0.40, "click": 0.20, "like": 0.15, "share": 0.05, "skip": 0.20},
        "dwell_mean_ms": 60_000,
        "dwell_std_ms":  30_000,
    },
    "power-user": {
        "preferred_categories": ["sci-fi", "drama", "comedy", "news", "sports", "documentary"],
        "event_weights": {"view": 0.25, "click": 0.25, "like": 0.20, "share": 0.15, "skip": 0.15},
        "dwell_mean_ms": 90_000,
        "dwell_std_ms":  50_000,
    },
}

USERS: list[dict[str, Any]] = []
archetype_keys = list(ARCHETYPES.keys())
for i in range(1, 101):
    archetype_name = archetype_keys[(i - 1) // 25]
    archetype      = ARCHETYPES[archetype_name]
    preferred      = archetype["preferred_categories"]
    # Build a weighted content_id pool: preferred 70 %, rest 30 %
    preferred_ids  = [c["content_id"] for c in CONTENT_CATALOGUE if c["category"] in preferred]
    other_ids      = [c for c in CONTENT_IDS if c not in preferred_ids]
    content_pool   = preferred_ids * 7 + other_ids * 3
    USERS.append({
        "user_id":      f"user_{i:03d}",
        "archetype":    archetype_name,
        "event_types":  list(archetype["event_weights"].keys()),
        "event_probs":  list(archetype["event_weights"].values()),
        "content_pool": content_pool,
        "dwell_mean":   archetype["dwell_mean_ms"],
        "dwell_std":    archetype["dwell_std_ms"],
    })

# ---------------------------------------------------------------------------
# Kafka Producer helpers
# ---------------------------------------------------------------------------

def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  500,
        "linger.ms":         5,
        "batch.size":        16_384,
    }
    return Producer(conf)


def delivery_report(err, msg) -> None:
    if err:
        logger.warning(f"Delivery failed: {err}")


def wait_for_kafka(max_retries: int = 30, delay: float = 5.0) -> None:
    """Poll Kafka until available."""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for attempt in range(max_retries):
        try:
            meta = admin.list_topics(timeout=5)
            logger.info(f"✓ Kafka is reachable – {len(meta.topics)} topics found.")
            return
        except KafkaException as exc:
            logger.warning(f"Kafka not ready ({attempt + 1}/{max_retries}): {exc}")
            time.sleep(delay)
    raise RuntimeError("Kafka did not become ready in time.")


# ---------------------------------------------------------------------------
# Content Metadata Publisher
# ---------------------------------------------------------------------------

def publish_content_metadata(producer: Producer) -> None:
    """Publish all content items to the compacted content-metadata topic."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for item in CONTENT_CATALOGUE:
        record = {
            "content_id":        item["content_id"],
            "category":          item["category"],
            "creator_id":        item["creator_id"],
            "publish_timestamp": now_iso,
        }
        producer.produce(
            topic=METADATA_TOPIC,
            key=item["content_id"].encode(),
            value=json.dumps(record).encode(),
            callback=delivery_report,
        )
    producer.flush()
    logger.info(f"✓ Published {len(CONTENT_CATALOGUE)} content metadata records.")


# ---------------------------------------------------------------------------
# Simulated Event Generator
# ---------------------------------------------------------------------------

def sim_now(start_real: float, start_sim: datetime) -> datetime:
    """Return current simulated timestamp."""
    elapsed_real   = time.time() - start_real
    elapsed_sim_s  = elapsed_real * TIME_ACCEL
    return start_sim + timedelta(seconds=elapsed_sim_s)


def generate_event(user: dict[str, Any], sim_ts: datetime, is_late: bool) -> dict[str, Any]:
    """Build a single user-event record."""
    if is_late:
        delay_s = random.randint(LATE_MIN_S, LATE_MAX_S)
        event_ts = sim_ts - timedelta(seconds=delay_s)
    else:
        # small jitter ±5 simulated seconds (within watermark tolerance)
        jitter_s = random.uniform(-5, 5)
        event_ts = sim_ts + timedelta(seconds=jitter_s)

    dwell = max(0, int(random.gauss(user["dwell_mean"], user["dwell_std"])))
    event_type = random.choices(user["event_types"], weights=user["event_probs"], k=1)[0]
    content_id = random.choice(user["content_pool"])

    return {
        "user_id":      user["user_id"],
        "content_id":   content_id,
        "event_type":   event_type,
        "dwell_time_ms": dwell,
        "timestamp":    event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("  Data Producer – Feature Engineering Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Kafka bootstrap         : {KAFKA_BOOTSTRAP}")
    logger.info(f"  user-events topic       : {USER_EVENTS_TOPIC}")
    logger.info(f"  content-metadata topic  : {METADATA_TOPIC}")
    logger.info(f"  Events per second       : {EVENTS_PER_SECOND}")
    logger.info(f"  Time acceleration       : {TIME_ACCEL}× (1 real s = {TIME_ACCEL} sim s)")
    logger.info(f"  Late event %            : {LATE_PCT * 100:.0f}%  ({LATE_MIN_S}–{LATE_MAX_S} sim s delay)")
    logger.info("=" * 60)

    wait_for_kafka()
    producer = make_producer()

    # Publish content metadata once at startup
    publish_content_metadata(producer)

    # Simulation clock starts at current real UTC time
    start_real = time.time()
    start_sim  = datetime.now(timezone.utc)

    interval   = 1.0 / EVENTS_PER_SECOND   # seconds between events (real time)
    total      = 0
    late_count = 0

    logger.info("Starting event simulation loop …")
    try:
        while True:
            loop_start = time.time()

            user     = random.choice(USERS)
            is_late  = random.random() < LATE_PCT
            sim_ts   = sim_now(start_real, start_sim)
            event    = generate_event(user, sim_ts, is_late)

            producer.produce(
                topic=USER_EVENTS_TOPIC,
                key=event["user_id"].encode(),
                value=json.dumps(event).encode(),
                callback=delivery_report,
            )
            producer.poll(0)    # trigger callbacks without blocking

            total      += 1
            late_count += int(is_late)

            if total % 500 == 0:
                late_pct = late_count / total * 100
                logger.info(
                    f"Produced {total} events  |  "
                    f"late={late_count} ({late_pct:.1f}%)  |  "
                    f"sim-time={sim_ts.strftime('%H:%M:%S')}  |  "
                    f"user={user['user_id']}"
                )

            # Rate-limit to EVENTS_PER_SECOND
            elapsed = time.time() - loop_start
            sleep   = interval - elapsed
            if sleep > 0:
                time.sleep(sleep)

    except KeyboardInterrupt:
        logger.info("Shutting down producer …")
    finally:
        producer.flush()
        logger.info(f"Flushed. Total events produced: {total}  |  Late: {late_count}")


if __name__ == "__main__":
    main()
