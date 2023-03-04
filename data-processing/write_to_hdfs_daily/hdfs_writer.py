from datetime import datetime
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

with open('../config/config.yml', 'r') as f:
    cfg = yaml.safe_load(f)

# Hbase config
HDFS_PATH = f"hdfs://{cfg['hdfs']['host']}:{cfg['hdfs']['port']}" + cfg['hdfs']['path']

# Kafka config
BROKER_URL = cfg['kafka']['broker_url']
TOPIC_NAME = cfg['kafka']['topic_name']

# Spark config
spark_config = cfg['spark']
SPARK_APP_NAME = spark_config['app_name']
SPARK_MASTER = spark_config['master']
SPARK_LOG_LEVEL = spark_config['log_level']
SPARK_BATCH_INTERVAL = spark_config['batch_interval']

def write_to_hdfs(df, epoch_id):
    current_date = datetime.now().strftime("%Y-%m-%d")
    partition_path = HDFS_PATH + current_date
    df.write.mode("append").parquet(partition_path)

def create_spark_context(app_name=SPARK_APP_NAME, master=SPARK_MASTER, log_level=SPARK_LOG_LEVEL, batch_interval=SPARK_BATCH_INTERVAL):
    """
    Create a new SparkContext with the given app name, master URL, log level, and batch interval.
    """
    conf = SparkConf().setAppName(app_name).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel(log_level)
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.streaming.pollingInterval", batch_interval)
    return spark


if __name__ == '__main__':
    spark = create_spark_context()

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BROKER_URL) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING) as message")
    df = df.withColumn("datetime", F.to_timestamp(F.regexp_extract("message", r'\[(.*?)\]', 1), "dd/MMM/yyyy:HH:mm:ss Z"))
    df = df.withColumn("http_method", F.split(F.split("message", " ")[2], "/")[0])
    df = df.withColumn("status_code", F.split("message", " ")[-2])
    df = df.withColumn("content_size", F.split("message", " ")[-1])
    df = df.select(["datetime", "http_method", "status_code", "content_size"])
    
    query = df.writeStream \
        .trigger(processingTime='1 minute') \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .foreachBatch(write_to_hdfs) \
        .start()

    query.awaitTermination()
    spark.stop()
