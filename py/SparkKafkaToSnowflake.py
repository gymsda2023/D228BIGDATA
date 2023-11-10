#SThis code continuously consumes messages from the Kafka topic, processes them using Spark, and saves the data to Snowflake.
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from confluent_kafka import Consumer, KafkaError, Producer

# Initialize a Spark session
spark = SparkSession.builder.appName("StockStreaming").getOrCreate()

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': '127.0.0.1:9092',  # Kafka broker address
    'group.id': 'stock-data-consumer',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start from the earliest offset
}

# Define the Kafka topic to consume from
kafka_topic = 'stock-streaming-topic'

# Create a Kafka consumer
consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])

# Define the schema for the incoming JSON messages (customize as per your data)
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Snowflake configuration
snowflake_options = {
    "sfURL": "YOUR_SNOWFLAKE_URL",
    "sfDatabase": "YOUR_DATABASE",
    "sfWarehouse": "YOUR_WAREHOUSE",
    "sfRole": "YOUR_ROLE",
    "sfSchema": "YOUR_SCHEMA",
}

# Consume and process Kafka messages
while True:
    message = consumer.poll(1.0)

    if message is None:
        continue

    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            print(f'Reached end of partition {message.partition()}.')
        else:
            print(f'Error while consuming message: {message.error()}')
    else:
        # Decode and parse the JSON message
        msg_value = message.value().decode('utf-8')
        msg_df = spark.read.json(spark.sparkContext.parallelize([msg_value]), schema=schema)

        # Perform any necessary data transformations or processing here
        # For example, you can aggregate data, perform calculations, etc.
        # For this example, we'll simply display the received data
        msg_df.show()

        # Save the DataFrame to Snowflake using the defined options
        msg_df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .mode("append") \
            .save()

# Stop Spark session (this won't execute in an infinite loop)
spark.stop()
