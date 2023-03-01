import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_date
from utils.helper import parse_log_line
from utils.hbase_helper import HBaseHelper

def process_logs(spark, input_path):
    # Define the schema for the log data
    log_schema = StructType([
        StructField('ip', StringType(), True),
        StructField('client', StringType(), True),
        StructField('user', StringType(), True),
        StructField('datetime', StringType(), True),
        StructField('request', StringType(), True),
        StructField('status', StringType(), True),
        StructField('size', StringType(), True),
        StructField('referer', StringType(), True)
    ])
