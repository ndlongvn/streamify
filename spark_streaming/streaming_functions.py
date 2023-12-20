from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, col, year, udf
import os


@udf
def string_decode(s, encoding='utf-8'):
    if s:
        return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
                .decode('unicode-escape') # Perform the actual octal-escaping decode
                .encode('latin1')         # 1:1 mapping back to bytes
                .decode(encoding)         # Decode original encoding
                .strip('\"'))

    else:
        return s

def create_or_get_spark_session(app_name, stream= False, master="yarn"):
    """
    Creates or gets a Spark Session

    Parameters:
        app_name : str
            Pass the name of your app
        master : str
            Choosing the Spark master, yarn is the default
    Returns:
        spark: SparkSession
    """
    if stream:

        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master=master) \
            .config('credentials', '~/streamify/google_credentials.json') \
            .config('parentProject', os.getenv("GCP_PROJECT_ID", 'deft-manifest-406205')) \
            .getOrCreate()
    else:
        spark = (SparkSession
                .builder
                .appName(app_name)
                .master(master=master)
                .getOrCreate())

    return spark


def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Creates a kafka read stream

    Parameters:
        spark : SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        topic : str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default 
    Returns:
        read_stream: DataStreamReader
    """
    if isinstance(kafka_port, str) or isinstance(kafka_port, int):
        # print(f"Reading from: {kafka_address}:{kafka_port}")
        print("Reading from: {}:{}".format(kafka_address, kafka_port))
        read_stream = (spark
                    .readStream
                    .format("kafka")
                    # .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                    .option("kafka.bootstrap.servers", "{}:{}".format(kafka_address, kafka_port))
                    .option("failOnDataLoss", False)
                    .option("startingOffsets", starting_offset)
                    .option("subscribe", topic)
                    .load())
    if isinstance(kafka_port, list):
        # print(f"Reading from: {kafka_address}:{kafka_port[0]},{kafka_address}:{kafka_port[1]}")
        print("Reading from: {}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
        read_stream = (spark
                    .readStream
                    .format("kafka")
                    # .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port[0]},{kafka_address}:{kafka_port[1]}")
                    .option("kafka.bootstrap.servers", "{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
                    .option("failOnDataLoss", False)
                    .option("startingOffsets", starting_offset)
                    .option("subscribe", topic)
                    .load())


    return read_stream


def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch on value from the kafka message.
    convert ts to timestamp format and produce year, month, day,
    hour columns
    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
    Returns:
        stream: DataStreamReader
    """

    # read only value from the incoming message and convert the contents
    # inside to the passed schema
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(
                  from_json(col("value"), stream_schema).alias(
                      "data")
              )
              .select("data.*")
              )

    # Add month, day, hour to split the data into separate directories
    stream = (stream
              .withColumn("ts", (col("ts")/1000).cast("timestamp"))
              .withColumn("year", year(col("ts")))
              .withColumn("month", month(col("ts")))
              .withColumn("hour", hour(col("ts")))
              .withColumn("day", dayofmonth(col("ts")))
              )

    # rectify string encoding
    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                .withColumn("song", string_decode("song"))
                .withColumn("artist", string_decode("artist")) 
                )


    return stream


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="60 seconds", output_mode="append", file_format="parquet"):
    """
    Write the stream back to a file store

    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
        file_format : str
            parquet, csv, orc etc
        storage_path : str
            The file output path
        checkpoint_path : str
            The checkpoint location for spark
        trigger : str
            The trigger interval
        output_mode : str
            append, complete, update
    """
    "add options: 1 second"
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("month", "day", "hour")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream

