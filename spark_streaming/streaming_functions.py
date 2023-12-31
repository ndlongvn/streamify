from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, expr, year, udf, minute, second
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
    spark = (SparkSession
            .builder
            .appName(app_name)
            .master(master=master)
            .getOrCreate())

    return spark


def create_kafka_read_stream(spark, groupid, kafka_address, kafka_port, topic, starting_offset="earliest"):
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
                    .option("group.id", groupid)
                    .option("kafka.bootstrap.servers", "{}:{}".format(kafka_address, kafka_port))
                    .option("failOnDataLoss", False)
                    .option("startingOffsets", starting_offset)
                    .option("subscribe", topic)
                    .load())
    if isinstance(kafka_port, list):
        # print(f"Reading from: {kafka_address}:{kafka_port[0]},{kafka_address}:{kafka_port[1]}")
        if len(kafka_port) ==2:
            print("Reading from: {}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
            read_stream = (spark
                        .readStream
                        .format("kafka")
                        .option("group.id", groupid)
                        # .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port[0]},{kafka_address}:{kafka_port[1]}")
                        .option("kafka.bootstrap.servers", "{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
                        .option("failOnDataLoss", False)
                        .option("startingOffsets", starting_offset)
                        .option("subscribe", topic)
                        .load())
        else:
            print("Reading from: {}:{},{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1], kafka_address, kafka_port[2]))
            read_stream = (spark
                        .readStream
                        .format("kafka")
                        .option("group.id", groupid)
                        # .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port[0]},{kafka_address}:{kafka_port[1]},{kafka_address}:{kafka_port[2]}")
                        .option("kafka.bootstrap.servers", "{}:{},{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1], kafka_address, kafka_port[2]))
                        .option("failOnDataLoss", False)
                        .option("startingOffsets", starting_offset)
                        .option("subscribe", topic)
                        .load())


    return read_stream


# def process_stream(stream, stream_schema, topic):
#     """
#     Process stream to fetch on value from the kafka message.
#     convert ts to timestamp format and produce year, month, day,
#     hour columns
#     Parameters:
#         stream : DataStreamReader
#             The data stream reader for your stream
#     Returns:
#         stream: DataStreamReader
#     """

#     # read only value from the incoming message and convert the contents
#     # inside to the passed schema
#     stream = (stream
#               .selectExpr("CAST(value AS STRING)")
#               .select(
#                   from_json(col("value"), stream_schema).alias(
#                       "data")
#               )
#               .select("data.*")
#               )

#     # Add month, day, hour to split the data into separate directories
#     stream = (stream
#               .withColumn("ts", (col("ts")/1000).cast("timestamp"))
#               .withColumn("year", year(col("ts")))
#               .withColumn("month", month(col("ts")))
#               .withColumn("hour", hour(col("ts")))
#               .withColumn("day", dayofmonth(col("ts")))
#               .withColumn("minute", minute(col("ts")))
#                 .withColumn("sec", sec(col("ts")))
#               )

#     # rectify string encoding
#     if topic in ["listen_events", "page_view_events"]:
#         stream = (stream
#                 .withColumn("song", string_decode("song"))
#                 .withColumn("artist", string_decode("artist")) 
#                 )


#     return stream


# def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="120 seconds", output_mode="append", file_format="parquet"):
#     """
#     Write the stream back to a file store

#     Parameters:
#         stream : DataStreamReader
#             The data stream reader for your stream
#         file_format : str
#             parquet, csv, orc etc
#         storage_path : str
#             The file output path
#         checkpoint_path : str
#             The checkpoint location for spark
#         trigger : str
#             The trigger interval
#         output_mode : str
#             append, complete, update
#     """
#     "add options: 1 second"
#     # write_stream = (stream
#     #                 .writeStream
#     #                 .format(file_format)
#     #                 .partitionBy("month", "day", "hour")
#     #                 .option("path", storage_path)
#     #                 .option("checkpointLocation", checkpoint_path)
#     #                 .trigger(processingTime=trigger)
#     #                 .outputMode(output_mode))
#     stream = stream.withColumn(
#             "five_minute_interval",
#             expr("concat(date_format(timestamp, 'yyyy-MM-dd HH:'), floor(minute(timestamp) / 5) * 5)"))

#     write_stream = (stream
#                     .writeStream
#                     .format(file_format)
#                     # Partition by the new 5-minute interval column along with month, day, hour if needed
#                     .partitionBy("month", "day", "hour", "five_minute_interval")
#                     .option("path", storage_path)
#                     .option("checkpointLocation", checkpoint_path)
#                     .trigger(processingTime=trigger)
#                     .outputMode(output_mode))

#     return write_stream

def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch value from the kafka message, convert ts to timestamp format
    and produce year, month, day, hour, and five-minute interval columns.
    Parameters:
        stream: DataStreamReader
            The data stream reader for your stream
        stream_schema: StructType
            The schema to apply to the data stream
    Returns:
        stream: DataFrame
    """

    # Extract data using the provided schema
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(from_json(col("value"), stream_schema).alias("data"))
              .select("data.*")
              )

    # Convert epoch time to timestamp and extract time components
    stream = (stream
              .withColumn("ts", (col("ts") / 1000).cast("timestamp"))
              .withColumn("year", year("ts"))
              .withColumn("month", month("ts"))
              .withColumn("day", dayofmonth("ts"))
              .withColumn("hour", hour("ts"))
            #   .withColumn("minute", minute("ts"))
            #   .withColumn("second", second("ts"))
              # Create a column for 5-minute time intervals
              .withColumn("five_minute_interval",
                          expr("concat(date_format(ts, 'yyyy-MM-dd HH-'), lpad(floor(minute(ts) / 5) * 5, 2, '0'))"))
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
        stream: DataFrame
            The data frame representing your stream
        storage_path: str
            The file output path
        checkpoint_path: str
            The checkpoint location for Spark
        trigger: str
            The trigger interval
        output_mode: str
            append, complete, update
        file_format: str
            parquet, csv, orc, etc.
    Returns:
        write_stream: DataStreamWriter
    """

    # Write out the stream partitioned by time components and 5-minute intervals
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("year", "month", "day", "hour", "five_minute_interval")  # Partition by the desired time components
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream

