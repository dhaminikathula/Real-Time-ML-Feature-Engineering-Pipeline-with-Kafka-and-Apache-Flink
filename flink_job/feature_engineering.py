"""
Real-Time ML Feature Engineering Pipeline
=========================================
Apache Flink Job using PyFlink Table API (SQL)

Time Characteristic  : EventTime
Watermark Strategy   : WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))

Features Computed
-----------------
1. click_rate        – TumblingEventTimeWindows.of(Time.hours(1))        per user
2. avg_dwell_time    – TumblingEventTimeWindows.of(Time.hours(1))        per user
3. engagement_rate   – SlidingEventTimeWindows.of(Time.minutes(15),
                                                   Time.minutes(5))      per content
4. category_affinity – Stream-Table Join + TumblingEventTimeWindows(1hr) per user-category

Output Topic : feature-store  (compacted, key = entity_id:feature_name)
"""

import os
import sys
import logging
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FeatureEngineering")


# ---------------------------------------------------------------------------
# Table Definitions
# ---------------------------------------------------------------------------

def create_user_events_table(t_env: StreamTableEnvironment, bootstrap: str, topic: str) -> None:
    """
    Kafka source for raw user interactions.

    EventTime processing is enabled through the WATERMARK clause.
    The strategy used is:
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
    which is expressed in SQL DDL as:
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '30' SECOND
    """
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS user_events (
            user_id       STRING,
            content_id    STRING,
            event_type    STRING,
            dwell_time_ms BIGINT,
            `timestamp`   TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '30' SECOND
        ) WITH (
            'connector'                        = 'kafka',
            'topic'                            = '{topic}',
            'properties.bootstrap.servers'     = '{bootstrap}',
            'properties.group.id'              = 'flink-feature-eng-group',
            'scan.startup.mode'                = 'earliest-offset',
            'format'                           = 'json',
            'json.timestamp-format.standard'   = 'ISO-8601',
            'json.fail-on-missing-field'       = 'false',
            'json.ignore-parse-errors'         = 'true'
        )
    """)
    logger.info(
        "✓ user_events table created "
        "(EventTime | BoundedOutOfOrderness watermark = 30 s)"
    )


def create_content_metadata_table(t_env: StreamTableEnvironment, bootstrap: str, topic: str) -> None:
    """
    Kafka source for content metadata.
    Used as the 'table' side in the Stream-Table join for category_affinity.
    """
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS content_metadata (
            content_id        STRING,
            category          STRING,
            creator_id        STRING,
            publish_timestamp STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = '{topic}',
            'properties.bootstrap.servers'  = '{bootstrap}',
            'properties.group.id'           = 'flink-metadata-group',
            'scan.startup.mode'             = 'earliest-offset',
            'format'                        = 'json',
            'json.fail-on-missing-field'    = 'false',
            'json.ignore-parse-errors'      = 'true'
        )
    """)
    logger.info("✓ content_metadata table created (stream-table join source)")


def create_feature_store_sink(t_env: StreamTableEnvironment, bootstrap: str, topic: str) -> None:
    """
    Kafka sink for computed features.

    Message key   : entity_key  (raw string = entity_id + ':' + feature_name)
    Message value : entity_id, feature_name, feature_value, computed_at
                    (entity_key is excluded from value via value.fields-include=EXCEPT_KEY)
    """
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS feature_store_sink (
            entity_key    STRING,
            entity_id     STRING,
            feature_name  STRING,
            feature_value DOUBLE,
            computed_at   STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'key.format'                   = 'raw',
            'key.fields'                   = 'entity_key',
            'value.format'                 = 'json',
            'value.fields-include'         = 'EXCEPT_KEY',
            'properties.acks'              = 'all'
        )
    """)
    logger.info("✓ feature_store_sink table created (key = entity_id:feature_name)")


# ---------------------------------------------------------------------------
# Feature Computations
# ---------------------------------------------------------------------------

def add_user_click_rate(stmt_set) -> None:
    """
    click_rate: proportion of 'click' events to total events per user.
    Window: TumblingEventTimeWindows.of(Time.hours(1))
    SQL:    TUMBLE(TABLE user_events, DESCRIPTOR(`timestamp`), INTERVAL '1' HOUR)
    """
    stmt_set.add_insert_sql("""
        INSERT INTO feature_store_sink
        SELECT
            CONCAT(user_id, ':', 'click_rate')                                  AS entity_key,
            user_id                                                              AS entity_id,
            'click_rate'                                                         AS feature_name,
            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE SUM(CASE WHEN event_type = 'click' THEN 1.0 ELSE 0.0 END)
                     / CAST(COUNT(*) AS DOUBLE)
            END                                                                  AS feature_value,
            CONCAT(
                DATE_FORMAT(window_end, 'yyyy-MM-dd'),
                'T',
                DATE_FORMAT(window_end, 'HH:mm:ss'),
                'Z'
            )                                                                    AS computed_at
        FROM TABLE(
            TUMBLE(TABLE user_events, DESCRIPTOR(`timestamp`), INTERVAL '1' HOUR)
        )
        GROUP BY user_id, window_start, window_end
    """)
    logger.info("  + click_rate         [TumblingEventTimeWindows – 1 hour]")


def add_user_avg_dwell_time(stmt_set) -> None:
    """
    avg_dwell_time: average dwell_time_ms per user.
    Window: TumblingEventTimeWindows.of(Time.hours(1))
    SQL:    TUMBLE(TABLE user_events, DESCRIPTOR(`timestamp`), INTERVAL '1' HOUR)
    """
    stmt_set.add_insert_sql("""
        INSERT INTO feature_store_sink
        SELECT
            CONCAT(user_id, ':', 'avg_dwell_time')                              AS entity_key,
            user_id                                                              AS entity_id,
            'avg_dwell_time'                                                     AS feature_name,
            COALESCE(AVG(CAST(dwell_time_ms AS DOUBLE)), 0.0)                   AS feature_value,
            CONCAT(
                DATE_FORMAT(window_end, 'yyyy-MM-dd'),
                'T',
                DATE_FORMAT(window_end, 'HH:mm:ss'),
                'Z'
            )                                                                    AS computed_at
        FROM TABLE(
            TUMBLE(TABLE user_events, DESCRIPTOR(`timestamp`), INTERVAL '1' HOUR)
        )
        GROUP BY user_id, window_start, window_end
    """)
    logger.info("  + avg_dwell_time     [TumblingEventTimeWindows – 1 hour]")


def add_content_engagement_rate(stmt_set) -> None:
    """
    engagement_rate: (likes + shares) / max(views, 1) per content item.
    Window: SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5))
    SQL:    HOP(TABLE user_events, DESCRIPTOR(`timestamp`),
                INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)

    Division-by-zero is handled: if no views, result = 0.
    """
    stmt_set.add_insert_sql("""
        INSERT INTO feature_store_sink
        SELECT
            CONCAT(content_id, ':', 'engagement_rate')                          AS entity_key,
            content_id                                                           AS entity_id,
            'engagement_rate'                                                    AS feature_name,
            CASE
                WHEN SUM(CASE WHEN event_type = 'view'  THEN 1.0 ELSE 0.0 END) = 0.0
                    THEN 0.0
                ELSE SUM(CASE WHEN event_type IN ('like','share') THEN 1.0 ELSE 0.0 END)
                     / SUM(CASE WHEN event_type = 'view' THEN 1.0 ELSE 0.0 END)
            END                                                                  AS feature_value,
            CONCAT(
                DATE_FORMAT(window_end, 'yyyy-MM-dd'),
                'T',
                DATE_FORMAT(window_end, 'HH:mm:ss'),
                'Z'
            )                                                                    AS computed_at
        FROM TABLE(
            HOP(TABLE user_events, DESCRIPTOR(`timestamp`),
                INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
        )
        GROUP BY content_id, window_start, window_end
    """)
    logger.info("  + engagement_rate    [SlidingEventTimeWindows – 15 min window, 5 min slide]")


def add_category_affinity_score(stmt_set) -> None:
    """
    category_affinity_<category>: count of interactions per (user, content-category).

    This implements the STREAM-TABLE JOIN requirement:
      - Left  stream : user_events  (infinite Kafka stream)
      - Right table  : content_metadata (read from content-metadata Kafka topic)
      - Join key     : content_id

    After enrichment the tumbling 1-hour window is applied using the
    old group-window syntax (TUMBLE in GROUP BY) which supports inner
    joins with non-windowed streaming tables.

    Window: TumblingEventTimeWindows.of(Time.hours(1))
    SQL:    GROUP BY ..., TUMBLE(u.`timestamp`, INTERVAL '1' HOUR)
    """
    stmt_set.add_insert_sql("""
        INSERT INTO feature_store_sink
        SELECT
            CONCAT(u.user_id, ':', CONCAT('category_affinity_', m.category))    AS entity_key,
            u.user_id                                                            AS entity_id,
            CONCAT('category_affinity_', m.category)                            AS feature_name,
            CAST(COUNT(*) AS DOUBLE)                                             AS feature_value,
            CONCAT(
                DATE_FORMAT(TUMBLE_END(u.`timestamp`, INTERVAL '1' HOUR), 'yyyy-MM-dd'),
                'T',
                DATE_FORMAT(TUMBLE_END(u.`timestamp`, INTERVAL '1' HOUR), 'HH:mm:ss'),
                'Z'
            )                                                                    AS computed_at
        FROM user_events AS u
        JOIN content_metadata AS m ON u.content_id = m.content_id
        GROUP BY
            u.user_id,
            m.category,
            TUMBLE(u.`timestamp`, INTERVAL '1' HOUR)
    """)
    logger.info("  + category_affinity  [Stream-Table Join + TumblingEventTimeWindows – 1 hour]")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    kafka_bootstrap        = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092")
    user_events_topic      = os.environ.get("USER_EVENTS_TOPIC",        "user-events")
    content_metadata_topic = os.environ.get("CONTENT_METADATA_TOPIC",   "content-metadata")
    feature_store_topic    = os.environ.get("FEATURE_STORE_TOPIC",      "feature-store")

    logger.info("=" * 65)
    logger.info("  Flink Feature Engineering Pipeline  –  Starting")
    logger.info("=" * 65)
    logger.info(f"  Kafka bootstrap  : {kafka_bootstrap}")
    logger.info(f"  Input topics     : {user_events_topic}, {content_metadata_topic}")
    logger.info(f"  Output topic     : {feature_store_topic}")
    logger.info("=" * 65)

    # ------------------------------------------------------------------
    # Stream Execution Environment
    # EventTime is configured through the WATERMARK DDL (see create_user_events_table).
    # Equivalent to: env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    # ------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60_000)          # checkpoint every 60 s

    t_env = StreamTableEnvironment.create(env)

    # State TTL to prevent unbounded growth (2 hours)
    t_env.get_config().set("table.exec.state.ttl", "7200000")
    # Idle source timeout so watermarks advance even when one partition is quiet
    t_env.get_config().set("table.exec.source.idle-timeout", "10000")

    # ------------------------------------------------------------------
    # Define Tables
    # ------------------------------------------------------------------
    create_user_events_table(t_env,      kafka_bootstrap, user_events_topic)
    create_content_metadata_table(t_env, kafka_bootstrap, content_metadata_topic)
    create_feature_store_sink(t_env,     kafka_bootstrap, feature_store_topic)

    # ------------------------------------------------------------------
    # Build Statement Set (all inserts run as one Flink job)
    # ------------------------------------------------------------------
    logger.info("Registering feature computations:")
    stmt_set = t_env.create_statement_set()

    add_user_click_rate(stmt_set)
    add_user_avg_dwell_time(stmt_set)
    add_content_engagement_rate(stmt_set)
    add_category_affinity_score(stmt_set)

    # ------------------------------------------------------------------
    # Submit Job
    # ------------------------------------------------------------------
    logger.info("Submitting job to Flink cluster …")
    try:
        table_result = stmt_set.execute()
        job_client   = table_result.get_job_client()
        if job_client:
            logger.info(f"✓ Job submitted  |  Job ID: {job_client.get_job_id()}")

        logger.info("Pipeline is RUNNING – streaming features to feature-store topic.")
        table_result.wait()          # blocks while job runs on the cluster

    except KeyboardInterrupt:
        logger.info("Shutdown requested – stopping pipeline.")
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
