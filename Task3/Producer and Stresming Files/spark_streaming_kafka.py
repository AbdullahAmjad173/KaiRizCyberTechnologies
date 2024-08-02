import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("PassengerId", StringType(), True),
    StructField("Pclass", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("SibSp", StringType(), True),
    StructField("Parch", StringType(), True),
    StructField("Ticket", StringType(), True),
    StructField("Fare", DoubleType(), True),
    StructField("Cabin", StringType(), True),
    StructField("Embarked", StringType(), True)
])

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "real-time-data") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Load pre-trained model
model = PipelineModel.load("E:/Internship/dataset/rf_model.pkl")

# Apply the model to the streaming data
predictions = model.transform(json_df)

# Select relevant columns and write to a file
query = predictions.select("PassengerId", "prediction") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "D:/Internship/dataset/predictions") \
    .option("checkpointLocation", "D:/Internship/dataset/checkpoints") \
    .start()

query.awaitTermination()
