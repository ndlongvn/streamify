from pyspark.sql.functions import window, col, count, avg
from streaming_functions import *
from schema import schema
import os

# streaming real-time data from kafka to bigquery
# 
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

KAFKA_PORT = ["9092", "9093"]

# KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
KAFKA_ADDRESS= "34.173.175.213"
GCP_GCS_BUCKET = "bigdata-project-it4931"
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime'
# Initialize Spark Session for Streaming

groupid= "realtime"

spark = create_or_get_spark_session('Realtime Streaming')
# Assuming the streams have been defined as per the provided schema and code:
# listen_events, page_view_events, auth_events

listen_events = create_kafka_read_stream(
    spark, groupid, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
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
    .option("checkpointLocation", "{}/checkpoint/{}".format(GCS_STORAGE_PATH, LISTEN_EVENTS_TOPIC)) \
    .option("temporaryGcsBucket", GCP_GCS_BUCKET) \
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
    .option("checkpointLocation", "{}/checkpoint/{}".format(GCS_STORAGE_PATH, LISTEN_EVENTS_TOPIC)) \
    .option("temporaryGcsBucket", GCP_GCS_BUCKET) \
    .outputMode("append") \
    .start() 


auth_events = create_kafka_read_stream(
    spark, groupid, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
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
    .option("temporaryGcsBucket", GCP_GCS_BUCKET) \
    .option("checkpointLocation", "{}/checkpoint/{}".format(GCS_STORAGE_PATH, LISTEN_EVENTS_TOPIC)) \
    .outputMode("append") \
    .start() 

# Start the streaming computation and wait for any termination
spark.streams.awaitAnyTermination()