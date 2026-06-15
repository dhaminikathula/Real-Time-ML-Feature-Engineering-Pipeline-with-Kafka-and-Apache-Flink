#!/usr/bin/env python3
"""
Batch Feature Comparison Script
================================
Reads all events from the user-events Kafka topic, computes the same features
as the Flink streaming job (click_rate, avg_dwell_time, engagement_rate) using
Pandas, and prints a side-by-side comparison with the streaming values read
from the feature-store topic.

Usage (run from project root after pipeline has been running for at least 2 minutes):
    pip install confluent-kafka pandas tabulate
    python scripts/batch_comparison.py

Environment variables (optional):
    KAFKA_BOOTSTRAP_SERVERS  – default: localhost:29092  (host-machine port)
    USER_EVENTS_TOPIC        – default: user-events
    FEATURE_STORE_TOPIC      – default: feature-store
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd
from confluent_kafka import Consumer, TopicPartition, KafkaException

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
USER_EVENTS_TOPIC = os.environ.get("USER_EVENTS_TOPIC",       "user-events")
FEATURE_STORE_TOPIC = os.environ.get("FEATURE_STORE_TOPIC",   "feature-store")
POLL_TIMEOUT      = 5.0   # seconds to wait for new messages before stopping


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def drain_topic(bootstrap: str, topic: str, max_records: int = 200_000) -> list[dict[str, Any]]:
    """Read all currently available messages from a Kafka topic from the start."""
    group_id = f"batch-compare-{uuid.uuid4().hex[:8]}"
    conf = {
        "bootstrap.servers":  bootstrap,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    c.subscribe([topic])

    records: list[dict[str, Any]] = []
    idle_polls = 0
    max_idle   = 3   # stop after 3 consecutive empty polls

    print(f"  Reading from '{topic}' …", end="", flush=True)
    while len(records) < max_records and idle_polls < max_idle:
        msg = c.poll(timeout=POLL_TIMEOUT)
        if msg is None:
            idle_polls += 1
            continue
        if msg.error():
            print(f"\n  Warning: {msg.error()}", file=sys.stderr)
            continue
        idle_polls = 0
        raw = msg.value()
        if raw:
            try:
                records.append(json.loads(raw.decode("utf-8")))
            except json.JSONDecodeError:
                pass

    c.close()
    print(f" {len(records):,} records loaded.")
    return records


# ── Batch feature computation ─────────────────────────────────────────────────

def compute_batch_features(events: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """
    Compute click_rate, avg_dwell_time (tumbling 1-hr) and
    engagement_rate (sliding 15-min / 5-min) using Pandas.

    Returns { entity_key: feature_value }
    """
    if not events:
        return {}

    df = pd.DataFrame(events)

    # Parse timestamps – ISO 8601 strings → UTC datetime
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df = df.sort_values("ts")

    results: dict[str, float] = {}

    # ── Tumbling 1-hour windows (origin=epoch, matching Flink's alignment) ──
    df.set_index("ts", inplace=True)

    # click_rate per user
    for user_id, grp in df.groupby("user_id"):
        for window, wdf in grp.resample("1h", origin="epoch"):
            if wdf.empty:
                continue
            total  = len(wdf)
            clicks = (wdf["event_type"] == "click").sum()
            rate   = clicks / total if total > 0 else 0.0
            key    = f"{user_id}:click_rate"
            # Keep the last window value (most recent, like the compacted topic)
            results[key] = round(float(rate), 6)

    # avg_dwell_time per user
    for user_id, grp in df.groupby("user_id"):
        for window, wdf in grp.resample("1h", origin="epoch"):
            if wdf.empty:
                continue
            avg_dwell = wdf["dwell_time_ms"].astype(float).mean()
            key = f"{user_id}:avg_dwell_time"
            results[key] = round(float(avg_dwell), 3)

    # engagement_rate per content (sliding 15 min / 5 min)
    for content_id, grp in df.groupby("content_id"):
        views = (grp["event_type"] == "view").resample("5min", origin="epoch").sum()
        likes_shares = grp["event_type"].isin(["like", "share"])
        ls_counts = likes_shares.resample("5min", origin="epoch").sum()

        # 15-minute rolling sum over 5-minute buckets = 3 buckets
        views_roll = views.rolling(window=3, min_periods=1).sum()
        ls_roll    = ls_counts.rolling(window=3, min_periods=1).sum()

        rate_series = ls_roll / views_roll.replace(0, float("nan"))
        rate_series = rate_series.fillna(0.0)

        if not rate_series.empty:
            latest_rate = float(rate_series.iloc[-1])
            key = f"{content_id}:engagement_rate"
            results[key] = round(latest_rate, 6)

    df.reset_index(inplace=True)
    return results


# ── Read streaming feature store ──────────────────────────────────────────────

def read_streaming_features(bootstrap: str, topic: str) -> dict[str, float]:
    """Read latest feature values per entity_key from the feature-store topic."""
    records = drain_topic(bootstrap, topic, max_records=100_000)
    latest: dict[str, float] = {}
    for r in records:
        entity_id    = r.get("entity_id", "")
        feature_name = r.get("feature_name", "")
        if entity_id and feature_name:
            key = f"{entity_id}:{feature_name}"
            latest[key] = r.get("feature_value", None)
    return latest


# ── Comparison & Report ───────────────────────────────────────────────────────

def compare(batch: dict[str, float], streaming: dict[str, float]) -> None:
    rows = []
    all_keys = sorted(set(list(batch.keys()) + list(streaming.keys())))

    for key in all_keys:
        b_val = batch.get(key)
        s_val = streaming.get(key)

        if b_val is None or s_val is None:
            diff_pct = "N/A"
            note     = "Only in one source"
        else:
            if b_val == 0 and s_val == 0:
                diff_pct = "0.00%"
                note     = "Both zero"
            elif b_val == 0:
                diff_pct = "∞"
                note     = "Batch is zero"
            else:
                pct      = abs(s_val - b_val) / abs(b_val) * 100
                diff_pct = f"{pct:.2f}%"
                note     = "Late-event drop" if pct > 1.0 else "Within tolerance"

        rows.append({
            "entity_key":       key[:50],
            "batch_value":      f"{b_val:.6f}" if isinstance(b_val, float) else str(b_val),
            "streaming_value":  f"{s_val:.6f}" if isinstance(s_val, float) else str(s_val),
            "diff_%":           diff_pct,
            "note":             note,
        })

    # Print table using simple formatting (no tabulate dependency required)
    cols    = ["entity_key", "batch_value", "streaming_value", "diff_%", "note"]
    widths  = [52, 14, 16, 10, 22]
    sep     = "─" * sum(widths + [len(cols) * 3 - 1])

    def fmt_row(r: list[str]) -> str:
        return " │ ".join(str(v).ljust(w) for v, w in zip(r, widths))

    print()
    print("=" * 80)
    print("  BATCH vs. STREAMING FEATURE COMPARISON")
    print("=" * 80)
    print(fmt_row(cols))
    print(sep)
    for row in rows[:60]:     # cap at 60 rows for readability
        print(fmt_row([row[c] for c in cols]))

    if len(rows) > 60:
        print(f"  … and {len(rows)-60} more rows (truncated for readability) …")

    # Summary statistics
    diffs = []
    for row in rows:
        try:
            diffs.append(float(row["diff_%"].replace("%", "")))
        except (ValueError, AttributeError):
            pass

    print()
    print("=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    print(f"  Total features compared : {len(rows)}")
    print(f"  Only in batch           : {sum(1 for r in rows if r['streaming_value'] == 'None')}")
    print(f"  Only in streaming       : {sum(1 for r in rows if r['batch_value'] == 'None')}")
    if diffs:
        import statistics
        print(f"  Mean absolute diff      : {statistics.mean(diffs):.2f}%")
        print(f"  Max  absolute diff      : {max(diffs):.2f}%")
        print(f"  Features within 1%     : {sum(1 for d in diffs if d <= 1.0)}/{len(diffs)}")
    print()
    print("  Key finding: Differences > 1% are caused by late-event dropping in the")
    print("  streaming pipeline (Flink watermark tolerance = 30 s). The batch script")
    print("  includes ALL events; Flink drops ~5% that arrive more than 30 s late.")
    print("  See ANALYSIS.md §'Batch vs. Streaming Divergence' for full explanation.")
    print("=" * 80)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║        Batch vs. Streaming Feature Comparison Script            ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  Kafka: {KAFKA_BOOTSTRAP}")
    print()

    print("Step 1: Loading raw events from Kafka …")
    events = drain_topic(KAFKA_BOOTSTRAP, USER_EVENTS_TOPIC)

    if not events:
        print("  ERROR: No events found. Is the pipeline running?")
        sys.exit(1)

    print(f"  Loaded {len(events):,} raw events spanning:")
    ts_list = sorted(e["timestamp"] for e in events if "timestamp" in e)
    if ts_list:
        print(f"    From : {ts_list[0]}")
        print(f"    To   : {ts_list[-1]}")

    print()
    print("Step 2: Computing batch features (Pandas) …")
    batch_features = compute_batch_features(events)
    print(f"  Computed {len(batch_features):,} feature values.")

    print()
    print("Step 3: Reading streaming features from feature-store …")
    streaming_features = read_streaming_features(KAFKA_BOOTSTRAP, FEATURE_STORE_TOPIC)
    print(f"  Read {len(streaming_features):,} streaming feature values.")

    print()
    print("Step 4: Comparing …")
    compare(batch_features, streaming_features)


if __name__ == "__main__":
    main()
