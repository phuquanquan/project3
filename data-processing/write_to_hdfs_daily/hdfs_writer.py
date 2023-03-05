from datetime import datetime
import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

class HDFSWriter:
    def __init__(self, cfg):
        self.hdfs_path = f"hdfs://{cfg['hdfs']['host']}:{cfg['hdfs']['port']}" + cfg['hdfs']['path']
        self.broker_url = cfg['kafka']['broker_url']
        self.topic_name = cfg['kafka']['topic_name']
        self.spark_app_name = cfg['spark']['app_name']
        self.spark_master = cfg['spark']['master']
        self.spark_log_level = cfg['spark']['log_level']
        self.spark_batch_interval = cfg['spark']['batch_interval']
    
    def write_to_hdfs(self, df, epoch_id):
        current_date = datetime.now().strftime("%Y-%m-%d")
        partition_path = self.hdfs_path + current_date
        df.write.mode("append").parquet(partition_path)
    
    def create_spark_context(self):
        """
        Create a new SparkContext with the given app name, master URL, log level, and batch interval.
        """
        conf = SparkConf().setAppName(self.spark_app_name).setMaster(self.spark_master)
        sc = SparkContext.getOrCreate(conf=conf)
        sc.setLogLevel(self.spark_log_level)
        spark = SparkSession(sc)
        spark.conf.set("spark.sql.streaming.pollingInterval", self.spark_batch_interval)
        return spark
    
    def run(self):
        spark = self.create_spark_context()

        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.broker_url) \
            .option("subscribe", self.topic_name) \
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
            .foreachBatch(self.write_to_hdfs) \
            .start()

        query.awaitTermination()
        spark.stop()
        
if __name__ == '__main__':
    with open('../config/config.yml', 'r') as f:
        cfg = yaml.safe_load(f)
    writer = HDFSWriter(cfg)
    writer.run()
