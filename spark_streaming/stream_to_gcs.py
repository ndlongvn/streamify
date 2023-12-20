from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, avg
from streaming_functions import *
from schema import schema
import os

# 
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

KAFKA_PORT = ["9092", "9093"]

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'streamify')
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET
# Initialize Spark Session for Streaming


spark = create_or_get_spark_session('Song Streaming')
# Assuming the streams have been defined as per the provided schema and code:
# listen_events, page_view_events, auth_events

listen_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# Example: Windowed Aggregation for Listening Trends
listen_events_trends = listen_events \
    .withWatermark("ts", "10 minutes") \
    .groupBy(window(col("ts"), "5 minutes"), col("song")) \
    .count() \
    .select("window.start", "window.end", "song", "count")


listen_events_trends.writeStream \
    .format("bigquery") \
    .option("table", "{}:{}.{}".format(os.getenv("GCP_PROJECT_ID", 'deft-manifest-406205'), "streamify_prod", "real_listen")) \
    .option("temporaryGcsBucket", os.getenv("GCP_GCS_BUCKET", 'streamify')) \
    .outputMode("append") \
    .start() \
    .awaitTermination()


# Example: User Engagement using Average Session Duration
user_engagement = listen_events \
    .groupBy("userId") \
    .agg(avg("duration").alias("avg_session_duration"))

user_engagement.writeStream \
    .format("bigquery") \
    .option("table", "{}:{}.{}".format(os.getenv("GCP_PROJECT_ID", 'deft-manifest-406205'), "streamify_prod", "real_user")) \
    .option("temporaryGcsBucket", os.getenv("GCP_GCS_BUCKET", 'streamify')) \
    .outputMode("append") \
    .start() \
    .awaitTermination()


auth_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)
# Example: Authentication Success Rate
auth_success_rate = auth_events \
    .groupBy(window(col("ts"), "5 minutes")) \
    .agg(
        (count(col("success")) / count("*")).alias("success_rate")
    ) \
    .select("window.start", "window.end", "success_rate")


auth_success_rate.writeStream \
    .format("bigquery") \
    .option("table", "{}:{}.{}".format(os.getenv("GCP_PROJECT_ID", 'deft-manifest-406205'), "streamify_prod", "real_auth")) \
    .option("temporaryGcsBucket", os.getenv("GCP_GCS_BUCKET", 'streamify')) \
    .outputMode("append") \
    .start() \
    .awaitTermination()


# Start the streaming computation and wait for any termination
spark.streams.awaitAnyTermination()