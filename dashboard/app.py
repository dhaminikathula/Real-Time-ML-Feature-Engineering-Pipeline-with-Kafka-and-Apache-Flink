"""
Observability Dashboard – Feature Engineering Pipeline
======================================================
FastAPI backend that:
  1. Consumes the feature-store Kafka topic to maintain an in-memory
     feature store (latest value per entity_key).
  2. Consumes the user-events Kafka topic to estimate:
       - Late events counter  (events with timestamp > 30 s behind the max seen)
       - Watermark lag        (wall-clock minus the maximum event timestamp seen)
  3. Serves a WebSocket endpoint that pushes real-time updates to the UI.
  4. Serves a REST endpoint for entity-feature lookup.
  5. Serves a static HTML dashboard (single-page app).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
USER_EVENTS_TOPIC     = os.environ.get("USER_EVENTS_TOPIC",        "user-events")
FEATURE_STORE_TOPIC   = os.environ.get("FEATURE_STORE_TOPIC",      "feature-store")
WATERMARK_TOLERANCE_S = 30    # must match Flink job watermark setting

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Dashboard")

# ---------------------------------------------------------------------------
# Shared In-Memory State  (thread-safe via GIL for simple dict ops)
# ---------------------------------------------------------------------------
# { "entity_key": { entity_id, feature_name, feature_value, computed_at, received_at } }
feature_store: dict[str, dict[str, Any]] = {}

metrics: dict[str, Any] = {
    "late_events_count":           0,
    "total_events_seen":           0,
    "max_event_timestamp":         None,
    "max_event_ts_epoch":          0.0,
    "watermark_lag_s":             None,
    "last_click_rate_at":          None,
    "last_engagement_rate_at":     None,
    "click_rate_freshness_s":      None,
    "engagement_rate_freshness_s": None,
    "pipeline_started_at":         datetime.now(timezone.utc).isoformat(),
}

connected_ws: set[WebSocket] = set()

# ---------------------------------------------------------------------------
# Main event loop reference  (set at startup, used by background threads)
# ---------------------------------------------------------------------------
_main_loop: Optional[asyncio.AbstractEventLoop] = None


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app       = FastAPI(title="ML Feature Pipeline Dashboard", version="1.0.0")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health():
    return {"status": "ok", "service": "dashboard"}


@app.get("/api/features/{entity_id}")
async def get_features(entity_id: str):
    """Return all latest feature values for a given entity_id."""
    result = [
        val for val in feature_store.values()
        if val.get("entity_id") == entity_id
    ]
    return {"entity_id": entity_id, "features": result}


@app.get("/api/metrics")
async def get_metrics():
    return metrics


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_ws.add(ws)
    logger.info(f"WebSocket connected  (total={len(connected_ws)})")
    try:
        # Send full current state immediately on connect
        await ws.send_json({
            "type":     "init",
            "features": list(feature_store.values()),
            "metrics":  metrics,
        })
        # Keep connection alive; client sends periodic pings
        while True:
            await asyncio.wait_for(ws.receive_text(), timeout=30)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception as exc:
        logger.debug(f"WebSocket error: {exc}")
    finally:
        connected_ws.discard(ws)
        logger.info(f"WebSocket disconnected (total={len(connected_ws)})")


# ---------------------------------------------------------------------------
# Broadcast helper  (must run on the main event loop)
# ---------------------------------------------------------------------------
async def _broadcast(payload: dict[str, Any]) -> None:
    dead: set[WebSocket] = set()
    for ws in list(connected_ws):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    connected_ws.difference_update(dead)


def broadcast_from_thread(payload: dict[str, Any]) -> None:
    """Thread-safe: schedule a broadcast on the main asyncio event loop."""
    if _main_loop and not _main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_broadcast(payload), _main_loop)


# ---------------------------------------------------------------------------
# Background: Feature-store Kafka consumer
# ---------------------------------------------------------------------------
def run_feature_store_consumer() -> None:
    """Reads feature-store topic; updates in-memory feature_store."""
    conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "dashboard-feature-store-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }
    c = Consumer(conf)
    c.subscribe([FEATURE_STORE_TOPIC])
    logger.info(f"Feature-store consumer started → {FEATURE_STORE_TOPIC}")

    while True:
        try:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"FeatureStore consumer error: {msg.error()}")
                continue

            raw = msg.value()
            if raw is None:
                continue

            try:
                record = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            entity_id    = record.get("entity_id",    "")
            feature_name = record.get("feature_name", "")
            if not entity_id or not feature_name:
                continue

            entity_key  = f"{entity_id}:{feature_name}"
            received_at = datetime.now(timezone.utc).isoformat()

            feature_store[entity_key] = {
                "entity_id":    entity_id,
                "feature_name": feature_name,
                "feature_value": record.get("feature_value"),
                "computed_at":  record.get("computed_at", ""),
                "received_at":  received_at,
                "entity_key":   entity_key,
            }

            _update_freshness_metrics(feature_name, record.get("computed_at", ""))

            broadcast_from_thread({
                "type":    "feature_update",
                "feature": feature_store[entity_key],
                "metrics": metrics,
            })

        except KafkaException as exc:
            logger.error(f"FeatureStore KafkaException: {exc}")
            time.sleep(2)
        except Exception as exc:
            logger.error(f"FeatureStore consumer error: {exc}", exc_info=True)
            time.sleep(1)


# ---------------------------------------------------------------------------
# Background: User-events Kafka consumer  (pipeline metrics)
# ---------------------------------------------------------------------------
def run_events_metrics_consumer() -> None:
    """
    Tracks max event timestamp → estimates watermark.
    Counts events that arrive > WATERMARK_TOLERANCE_S behind max → late events.
    """
    conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "dashboard-events-metrics-consumer",
        "auto.offset.reset":  "latest",   # only watch new events
        "enable.auto.commit": True,
    }
    c = Consumer(conf)
    c.subscribe([USER_EVENTS_TOPIC])
    logger.info(f"Events metrics consumer started → {USER_EVENTS_TOPIC}")

    while True:
        try:
            msg = c.poll(timeout=1.0)
            if msg is None:
                _refresh_watermark_lag()
                continue
            if msg.error():
                logger.warning(f"Events consumer error: {msg.error()}")
                continue

            raw = msg.value()
            if raw is None:
                continue

            try:
                event = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            metrics["total_events_seen"] += 1
            event_ts_str = event.get("timestamp", "")
            if not event_ts_str:
                continue

            try:
                event_epoch = _parse_ts(event_ts_str).timestamp()
            except ValueError:
                continue

            # Advance the max-seen timestamp
            if event_epoch > metrics["max_event_ts_epoch"]:
                metrics["max_event_ts_epoch"]  = event_epoch
                metrics["max_event_timestamp"] = event_ts_str

            # Late = more than watermark_tolerance behind the current max
            lag_s = metrics["max_event_ts_epoch"] - event_epoch
            if lag_s > WATERMARK_TOLERANCE_S:
                metrics["late_events_count"] += 1

            _refresh_watermark_lag()

        except KafkaException as exc:
            logger.error(f"Events consumer KafkaException: {exc}")
            time.sleep(2)
        except Exception as exc:
            logger.error(f"Events consumer error: {exc}", exc_info=True)
            time.sleep(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _update_freshness_metrics(feature_name: str, computed_at: str) -> None:
    now = datetime.now(timezone.utc)
    if not computed_at:
        return
    try:
        ts = _parse_ts(computed_at)
        age = round((now - ts).total_seconds(), 1)
        if feature_name == "click_rate":
            metrics["last_click_rate_at"]     = computed_at
            metrics["click_rate_freshness_s"] = age
        elif feature_name == "engagement_rate":
            metrics["last_engagement_rate_at"]     = computed_at
            metrics["engagement_rate_freshness_s"] = age
    except Exception:
        pass


def _refresh_watermark_lag() -> None:
    """
    Watermark lag = wall_clock − (max_event_ts − watermark_tolerance).
    Positive → pipeline is behind wall clock.
    """
    if metrics["max_event_ts_epoch"] > 0:
        estimated_wm        = metrics["max_event_ts_epoch"] - WATERMARK_TOLERANCE_S
        metrics["watermark_lag_s"] = round(time.time() - estimated_wm, 1)


def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO 8601 variants into UTC datetime."""
    ts_str = ts_str.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts_str!r}")


# ---------------------------------------------------------------------------
# Periodic metrics push  (asyncio task on main loop)
# ---------------------------------------------------------------------------
async def periodic_metrics_broadcast() -> None:
    while True:
        await asyncio.sleep(2)
        if connected_ws:
            await _broadcast({"type": "metrics", "metrics": metrics})


# ---------------------------------------------------------------------------
# Application Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    global _main_loop
    # Capture the running event loop so background threads can schedule on it
    _main_loop = asyncio.get_event_loop()

    logger.info("Dashboard starting …")
    logger.info(f"  Kafka bootstrap     : {KAFKA_BOOTSTRAP}")
    logger.info(f"  user-events topic   : {USER_EVENTS_TOPIC}")
    logger.info(f"  feature-store topic : {FEATURE_STORE_TOPIC}")

    threading.Thread(
        target=run_feature_store_consumer,
        daemon=True,
        name="FeatureStoreConsumer",
    ).start()

    threading.Thread(
        target=run_events_metrics_consumer,
        daemon=True,
        name="EventsMetricsConsumer",
    ).start()

    asyncio.create_task(periodic_metrics_broadcast())

    logger.info("Dashboard is ready ✓")
