from kafka import KafkaProducer
import gzip
import json
import time
import os
import yaml

with open('../config/config.yml', 'r') as f:
    cfg = yaml.safe_load(f)

file_path = cfg['log_files']['dir_path']

broker_url = cfg['kafka']['broker_url']
topic_name = cfg['kafka']['topic_name']
compression_type = cfg['kafka']['compression_type']
batch_size = cfg['kafka']['batch_size']
linger_ms = cfg['kafka']['linger_ms']
acks = cfg['kafka']['acks']

producer = KafkaProducer(
    bootstrap_servers=[broker_url],
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    compression_type=compression_type,
    batch_size=batch_size,
    linger_ms=linger_ms,
    acks=acks
)

with gzip.open(file_path, 'rt') as f:
    for line in f:
        producer.send(topic_name, value={"message": line.strip()})
        time.sleep(0.001)

producer.flush()
producer.close()
