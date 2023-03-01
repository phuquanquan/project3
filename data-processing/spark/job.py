from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import yaml

# Load config file
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Initialize SparkConf and SparkContext
conf = SparkConf().setAppName(config["spark"]["app_name"]).setMaster(config["spark"]["master"])
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Initialize StreamingContext
ssc = StreamingContext(sc, config["spark"]["batch_duration"])

# Create checkpoint directory
ssc.checkpoint(config["spark"]["checkpoint_directory"])

# Define Kafka params
kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "nasa_log_processing"
}

# Define topics
topics = ["nasa_logs"]

# Create input stream from Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

# Define processing function
def process_logs(rdd):
    logs = rdd.map(lambda x: x[1])
    # Your processing logic goes here
    print(logs.collect())

# Apply processing function to the stream
kafka_stream.foreachRDD(process_logs)

# Start streaming
ssc.start()
ssc.awaitTermination()
