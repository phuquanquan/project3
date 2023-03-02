import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

with open('../config/config.yml', 'r') as f:
    cfg = yaml.safe_load(f)

def process_web_logs(spark):
    # Define the schema for the web logs
    log_schema = StructType([
        StructField("host", StringType(), True),
        StructField("client_identd", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("date_time", StringType(), True),
        StructField("method", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("response_code", IntegerType(), True),
        StructField("content_size", IntegerType(), True)
    ])

    # Create the initial DataFrame representing the stream of web logs from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg['kafka']['broker_url']) \
        .option("subscribe", cfg['kafka']['topic_name']) \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert the value of each message to a string and extract the fields
    value_df = kafka_df.selectExpr("CAST(value AS STRING)")
    log_df = value_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                              regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('date_time'),
                              regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('endpoint'),
                              regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('response_code'),
                              regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))

    # Filter out records with missing values
    log_df = log_df.filter(col("host").isNotNull())
    
    # Create the view to run queries against
    log_df.createOrReplaceTempView("web_logs")

    # Compute statistics over a sliding window
    windowed_counts = spark.sql("""
        SELECT endpoint, window(date_time, "10 minutes").start AS window_start, count(*) AS view_count
        FROM web_logs
        GROUP BY endpoint, window(date_time, "10 minutes")
    """)

    # Write the results out to HBase
    hbase_url = cfg['hbase']['host'] + ":" + cfg['hbase']['port']
    hbase_table = cfg['hbase']['table_name']
    hbase_columns = {"cf": "view_count"}
    hbase_config = {
        "hbase.zookeeper.quorum": hbase_url,
        "hbase.mapred.outputtable": hbase_table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }

    windowed_counts \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_hbase(batch_df, batch_id, hbase_table, hbase_columns, hbase_config)) \
        .start() \
        .awaitTermination()

def write_to_hbase(df, epoch_id, table_name, column_family_mapping, config):
    """
    Write a batch DataFrame to an HBase table.
    """
    import happybase
    # Convert the DataFrame to a list of tuples that can be written to HBase
    row_list = df \
        .rdd \
        .map(lambda row: (
            row['endpoint'].encode('utf-8'),
            {column_family_mapping['cf'].encode('utf-8'): str(row['view_count']).encode('utf-8')}
        )) \
        .collect()

    # Write the rows to HBase
    with happybase.Connection(**config) as connection:
        table = connection.table(table_name)
        for row in row_list:
            table.put(row[0], row[1])


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("WebLogsStreaming").getOrCreate()
    # Set the log level to reduce the amount of output printed
    spark.sparkContext.setLogLevel(cfg['spark']['log_level'])

    # Process the web logs
    process_web_logs(spark)

    # Stop the SparkSession
    spark.stop()
