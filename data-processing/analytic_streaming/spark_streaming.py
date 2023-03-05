import yaml
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, window, count
from pyspark.sql.types import IntegerType
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


class SparkStreaming:
    def __init__(self, cfg):
        self.spark_config = cfg['spark']
        self.kafka_config = cfg['kafka']
        self.hbase_config = cfg['hbase']

    def create_spark_context(self):
        """
        Create a new SparkContext with the given app name, master URL, log level, and batch interval.
        """
        conf = SparkConf().setAppName(self.spark_config['app_name']).setMaster(self.spark_config['master'])
        sc = SparkContext.getOrCreate(conf=conf)
        sc.setLogLevel(self.spark_config['log_level'])
        spark = SparkSession(sc)
        spark.conf.set("spark.sql.streaming.pollingInterval", self.spark_config['batch_interval'])
        return spark

    @staticmethod
    def write_to_hbase(df, epoch_id, table_name, column_family_mapping, config):
        """
        Write a batch DataFrame to an HBase table.
        """
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

    def process_web_logs(self):
        # Create the initial DataFrame representing the stream of web logs from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['broker_url']) \
            .option("subscribe", self.kafka_config['topic_name']) \
            .option("startingOffsets", "earliest") \
            .load()

        # Convert the value of each message to a string and extract the fields
        value_df = kafka_df.selectExpr("CAST(value AS STRING)")
        log_df = value_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                                  regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('date_time'),
                                  regexp_extract('value',r'"(?:GET|POST|PUT|DELETE|HEAD) (.*?) HTTP', 1).alias('endpoint'),
                                  regexp_extract('value', r'\s(\d{3})\s', 1).cast(IntegerType()).alias('response_code'),
                                  regexp_extract('value', r'\s(\d+)$', 1).cast(IntegerType()).alias('content_size'))
        
         # Split the date_time column into separate date and time columns
        log_df = log_df.withColumn('date', split(col('date_time'), ':')[0])
        log_df = log_df.withColumn('time', split(col('date_time'), ':')[1])

        # Calculate the number of views per endpoint for each batch interval
        windowed_counts = log_df \
            .groupBy(
                window(col("date_time"), self.spark_config['batch_interval'], self.spark_config['slide_interval']),
                col('endpoint')
            ) \
            .agg(count('*').alias('view_count'))

        # Write the results to HBase
        windowed_counts.writeStream \
            .foreachBatch(lambda df, epoch_id: self.write_to_hbase(df, epoch_id, self.hbase_config['table_name'],
                                                                self.hbase_config['column_family_mapping'],
                                                                self.hbase_config['connection'])) \
            .start() \
            .awaitTermination()

    def run(self):
        self.spark = self.create_spark_context()
        self.process_web_logs()

if __name__ == '__main__':
    with open('../config/config.yml', 'r') as f:
        cfg = yaml.safe_load(f)
    ss = SparkStreaming(cfg)
    ss.run()
