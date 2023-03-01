from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import yaml

def read_config():
    with open("../config/config.yml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return cfg

def start_consumer():
    # read configuration file
    cfg = read_config()

    # set up Spark Streaming context
    sc = SparkContext(appName="LogProcessing")
    sc.setLogLevel(cfg['spark_log_level'])
    ssc = StreamingContext(sc, cfg['batch_interval'])

    # set up Kafka Consumer
    kafka_params = {"metadata.broker.list": cfg['kafka_broker_list']}
    kafka_topic = cfg['kafka_topic']
    stream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafka_params)

    # process each RDD
    stream.foreachRDD(process_rdd)

    # start the Streaming context
    ssc.start()
    ssc.awaitTermination()

def process_rdd(rdd):
    # filter out empty lines
    lines = rdd.filter(lambda line: len(line) > 0)

    # split each line into fields
    fields = lines.map(lambda line: line.split(' '))

    # do further processing on fields here, e.g. filtering, mapping, aggregation, etc.

    # save the processed data to HBase
    save_to_hbase(fields.collect())

def save_to_hbase(data):
    # save data to HBase
    pass

if __name__ == "__main__":
    start_consumer()
