import time
import json
import logging
import yaml
from kafka import KafkaProducer

with open('../config/config.yml', 'r') as f:
    config = yaml.safe_load(f)

broker_url = config['kafka']['broker_url']
topic_name = config['kafka']['topic_name']

producer = KafkaProducer(
    bootstrap_servers=[broker_url],
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

with open('../data_processing/log_files/input.log', 'r') as f:
    for line in f:
        producer.send(topic_name, line)
        time.sleep(1)
