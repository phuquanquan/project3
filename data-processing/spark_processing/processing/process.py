import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from utils.helper import parseApacheLogLine
from utils.hbase_helper import HBaseHelper

def parse_logs(spark, log_df):
    # Parse each log line into a dictionary of fields
    parsed_logs = log_df.rdd.map(lambda line: parseApacheLogLine(line[0])).filter(lambda x: x is not None)

    # Define the schema for the log data
    log_schema = StructType([
        StructField('host', StringType(), True),
        StructField('client_identd', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('date_time', StringType(), True),
        StructField('method', StringType(), True),
        StructField('endpoint', StringType(), True),
        StructField('protocol', StringType(), True),
        StructField('response_code', StringType(), True),
        StructField('content_size', StringType(), True)
    ])
    
    # Convert RDD to DataFrame
    logs_df = spark.createDataFrame(parsed_logs, schema=log_schema)


    return logs_df

def clean_logs(logs_df):
    """
    Clean log data by removing rows with missing or invalid data.
    """
    # Remove rows with missing or invalid data
    cleaned_logs = logs_df.filter(
        (logs_df.host.isNotNull()) &
        (logs_df.date_time.isNotNull()) &
        (logs_df.method.isNotNull()) &
        (logs_df.endpoint.isNotNull()) &
        (logs_df.protocol.isNotNull()) &
        (logs_df.response_code.isNotNull()) &
        (logs_df.content_size.isNotNull())
    )

    return cleaned_logs

def convert_datetime(logs_df):
    # Convert datetime string to TimestampType
    logs_df = logs_df.withColumn('date_time', F.to_timestamp(logs_df.datetime, 'dd/MMM/yyyy:HH:mm:ss Z'))

def write_to_hbase(logs_df):
    # Write to HBase
    helper = HBaseHelper()
    helper.write_to_hbase(logs_df.collect())

def process_logs(spark, input_path):
    # Read the log file
    log_df = spark.read.text(input_path)

     # Parse the log data
    logs_df = parse_logs(spark, log_df)

    # Clean the log data
    cleaned_logs = clean_logs(logs_df)

    # Convert datetime string to TimestampType
    logs_df = convert_datetime(cleaned_logs)

    # Write to HBase
    write_to_hbase(logs_df)
