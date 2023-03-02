from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, substring_index


# TODO: update with your HDFS configuration
HDFS_URL = "hdfs://<namenode_host>:<port>"
LOG_DIR = "/logs/"
LOG_FILE = "access.log"
LOG_PATH = HDFS_URL + LOG_DIR + LOG_FILE


def create_spark_session():
    spark = SparkSession.builder.appName("LogAnalyzer").getOrCreate()
    return spark


def read_from_hdfs(spark):
    logs_df = (
        spark.read.text(LOG_PATH)
        .select(
            to_timestamp(substring
