from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    count,
    desc,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVER = "18.61.53.176:9092"

KAFKA_TOPIC = "twitter"


# Unity Catalog Volume checkpoint locations
CHECKPOINT_BASE = (
    "/Volumes/workspace/default/social_media_trends/checkpoints"
)


# ============================================================
# DEFINE JSON SCHEMA
# ============================================================

social_media_schema = StructType([
    StructField("user", StringType(), True),

    StructField("text", StringType(), True),

    StructField(
        "hashtags",
        ArrayType(StringType()),
        True,
    ),

    StructField("timestamp", StringType(), True),
])


# ============================================================
# CONNECT TO KAFKA
# ============================================================

kafka_stream = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVER,
    )

    .option(
        "subscribe",
        KAFKA_TOPIC,
    )

    .option(
        "startingOffsets",
        "latest",
    )

    .load()
)


print("Kafka streaming DataFrame created successfully.")


# ============================================================
# CONVERT KAFKA VALUE TO STRING
# ============================================================

kafka_value_stream = kafka_stream.selectExpr(
    "CAST(value AS STRING) AS value"
)


# ============================================================
# PARSE JSON
# ============================================================

streaming_parsed = (
    kafka_value_stream
    .select(
        from_json(
            col("value"),
            social_media_schema,
        ).alias("data")
    )

    .select(
        col("data.user").alias("user"),
        col("data.text").alias("text"),
        col("data.hashtags").alias("hashtags"),
        col("data.timestamp").alias("timestamp"),
    )
)


print("Streaming data parsed successfully.")


# ============================================================
# DISPLAY RAW STREAMING DATA
# ============================================================

display(
    streaming_parsed,
    checkpointLocation=(
        f"{CHECKPOINT_BASE}/display_parsed"
    ),
)


# ============================================================
# STORE RAW STREAMING DATA AS DELTA TABLE
# ============================================================

raw_stream_query = (
    streaming_parsed
    .writeStream

    .format("delta")

    .outputMode("append")

    .option(
        "checkpointLocation",
        f"{CHECKPOINT_BASE}/raw_stream",
    )

    .toTable(
        "workspace.default.raw_social_media"
    )
)


print("Streaming ingestion started.")


# ============================================================
# TRENDING HASHTAGS
# ============================================================

hashtags_stream = (
    streaming_parsed

    .select(
        explode(
            col("hashtags")
        ).alias("hashtag")
    )
)


trending_hashtags = (
    hashtags_stream

    .groupBy(
        "hashtag"
    )

    .agg(
        count("*").alias("hashtag_count")
    )
)


trending_hashtags_query = (
    trending_hashtags
    .writeStream

    .outputMode("complete")

    .option(
        "checkpointLocation",
        f"{CHECKPOINT_BASE}/trending_hashtags",
    )

    .toTable(
        "workspace.default.trending_hashtags"
    )
)


print("Trending hashtag pipeline created successfully.")


# ============================================================
# MOST ACTIVE USERS
# ============================================================

active_users = (
    streaming_parsed

    .groupBy(
        "user"
    )

    .agg(
        count("*").alias("post_count")
    )
)


active_users_query = (
    active_users
    .writeStream

    .outputMode("complete")

    .option(
        "checkpointLocation",
        f"{CHECKPOINT_BASE}/active_users",
    )

    .toTable(
        "workspace.default.most_active_users"
    )
)


print("Most active users pipeline created successfully.")
