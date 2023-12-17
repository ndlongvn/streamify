from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "mytopic",
    "startingOffsets": "earliest"
}

# Read data from Kafka as a DataFrame
df = spark.read.format("kafka") \
    .options(**kafka_params) \
    .load()

# Perform further processing on the DataFrame
# ...

# Start the Spark streaming
spark.streams.awaitAnyTermination()