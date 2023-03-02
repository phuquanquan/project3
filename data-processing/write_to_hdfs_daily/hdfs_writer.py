from datetime import datetime
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

with open('../config/config.yml', 'r') as f:
    cfg = yaml.safe_load(f)

hdfs_host = cfg['hdfs']['host']
hdfs_port = cfg['hdfs']['port']
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}" + cfg['hdfs']['path']
broker_url = cfg['kafka']['broker_url']
topic_name = cfg['kafka']['topic_name']

def write_to_hdfs(df, epoch_id):
    current_date = datetime.now().strftime("%Y-%m-%d")
    partition_path = hdfs_path + current_date
    df.write.mode("append").parquet(partition_path)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("WebLogs").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", broker_url) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING) as message")
    df = df.withColumn("datetime", F.to_timestamp(F.regexp_extract("message", r'\[(.*?)\]', 1), "dd/MMM/yyyy:HH:mm:ss Z"))
    df = df.withColumn("http_method", F.split(F.split("message", " ")[0], "/")[0])
    df = df.withColumn("status_code", F.split("message", " ")[-2])
    df = df.withColumn("content_size", F.split("message", " ")[-1])
    df = df.select(["datetime", "http_method", "status_code", "content_size"])
    
    query = df.writeStream \
        .trigger(processingTime='1 minute') \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .foreachBatch(write_to_hdfs) \
        .start()

    query.awaitTermination()
