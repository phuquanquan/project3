import gzip
import yaml
from kafka import KafkaProducer

# Load config from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Set Kafka Producer config
producer_config = {
    "bootstrap_servers": [f"{config['kafka']['broker_host']}:{config['kafka']['broker_port']}"],
    "value_serializer": config['kafka']['producer']['value_serializer'],
    "compression_type": config['kafka']['producer']['compression_type']
}

# Create Kafka Producer instance
producer = KafkaProducer(**producer_config)

# Read data from file
with gzip.open("nasa_data.gzip", "rb") as f:
    for line in f:
        # Send each line to Kafka topic
        producer.send(config['kafka']['topic_name'], value=line.decode("utf-8"))

# Wait for all messages to be sent before exiting
producer.flush()
