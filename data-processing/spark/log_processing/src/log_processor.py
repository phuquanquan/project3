from pyspark.sql import SparkSession
from pyspark.sql.functions import split, from_unixtime, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

class LogProcessor:
    def __init__(self, spark, input_path, output_path):
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path
        
    def process_logs(self):
        # Define schema for the log data
        schema = StructType([
            StructField("host", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("request", StringType(), True),
            StructField("http_reply_code", StringType(), True),
            StructField("bytes", StringType(), True)
        ])

        # Read log data from file
        logs_df = self.spark.read.format("csv").option("header", False).schema(schema).load(self.input_path)

        # Parse timestamp column and create new columns for year, month, day, and hour
        logs_df = logs_df.withColumn("timestamp", from_unixtime(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"))
        logs_df = logs_df.withColumn("year", expr("substring(timestamp, 8, 4)"))
        logs_df = logs_df.withColumn("month", expr("substring(timestamp, 4, 3)"))
        logs_df = logs_df.withColumn("day", expr("substring(timestamp, 1, 2)"))
        logs_df = logs_df.withColumn("hour", expr("substring(timestamp, 13, 2)"))

        # Convert bytes column to double type
        logs_df = logs_df.withColumn("bytes", logs_df["bytes"].cast(DoubleType()))

        # Group data by year, month, day, and hour and calculate average bytes per request
        avg_bytes_df = logs_df.groupBy("year", "month", "day", "hour").agg({"bytes": "avg"})

        # Write results to output file
        avg_bytes_df.write.format("csv").mode("overwrite").save(self.output_path)
