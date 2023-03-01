import json
import yaml
from kafka import KafkaConsumer

# Load config from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Set Kafka Consumer config
consumer_config = {
    "bootstrap_servers": [f"{config['kafka']['broker_host']}:{config['kafka']['broker_port']}"],
    "value_deserializer": config['kafka']['consumer']['value_deserializer']
}

# Create Kafka Consumer instance
consumer = KafkaConsumer(config['kafka']['topic_name'], **consumer_config)

# Consume messages from Kafka topic
for message in consumer:
    # Parse JSON message
    data = json.loads(message.value)
    # Process data here
    print(data)
