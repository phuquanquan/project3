from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import yaml
import happybase


def read_config():
    with open("data_processing/config/config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg


def connect_to_hbase():
    # read configuration file
    cfg = read_config()

    # connect to HBase
    connection = happybase.Connection(cfg['hbase']['host'], cfg['hbase']['port'])
    connection.open()

    # create table if not exists
    table_name = cfg['hbase']['table_name']
    families = {'log': dict()}
    if table_name not in connection.tables():
        connection.create_table(table_name, families)

    # return table instance
    return connection.table(table_name)


def start_consumer():
    # read configuration file
    cfg = read_config()

    # set up Spark Streaming context
    sc = SparkContext(appName="LogProcessing")
    sc.setLogLevel(cfg['spark']['log_level'])
    ssc = StreamingContext(sc, cfg['spark']['batch_interval'])

    # set up Kafka Consumer
    kafka_params = {"metadata.broker.list": cfg['kafka']['broker_url']}
    kafka_topic = cfg['kafka']['topic_name']
    stream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafka_params)

    # process each RDD
    stream.foreachRDD(lambda rdd: process_rdd(rdd, connect_to_hbase()))

    # start the Streaming context
    ssc.start()
    ssc.awaitTermination()


def process_rdd(rdd, table):
    # filter out empty lines
    lines = rdd.filter(lambda line: len(line) > 0)

    # split each line into fields
    fields = lines.map(lambda line: line.split(' '))

    # do further processing on fields here, e.g. filtering, mapping, aggregation, etc.

    # save the processed data to HBase
    fields.foreach(lambda parts: save_to_hbase(parts, table))


def save_to_hbase(parts, table):
    # Parse log line
    host = parts[0]
    client_identd = parts[1]
    user_id = parts[2]
    date_time = parts[3] + ' ' + parts[4]
    method = parts[5]
    endpoint = parts[6]
    protocol = parts[7]
    response_code = parts[8]
    content_size = ' '.join(parts[9:])

    # Create row key
    row_key = '{0}-{1}'.format(host, date_time)

    # Create dictionary of values to insert
    values = {
        'log:client_identd': client_identd,
        'log:user_id': user_id,
        'log:method': method,
        'log:endpoint': endpoint,
        'log:protocol': protocol,
        'log:response_code': response_code,
        'log:content_size': content_size
    }

    # Insert row into HBase table
    table.put(row_key, values)


if __name__ == "__main__":
    start_consumer()
