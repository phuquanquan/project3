import json
import random
import time
import ipaddress
from kafka import KafkaProducer
import yaml

def read_config():
    with open('../config/config.yml', 'r') as f:
        return yaml.safe_load(f)
    
def generate_random_ip_address():
    while True:
        ip = ipaddress.IPv4Address(random.randint(0x0B000000, 0xDF000000))
        if ip.is_global and not ip.is_multicast:
            return str(ip)

def create_log_message():
    http_methods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"]
    endpoints = ["/", "/about", "/contact", "/login", "/logout", "/api", "/dashboard"]

    host = generate_random_ip_address()
    client_identd = '-'
    user_id = '-'
    date_time = time.strftime("[%d/%b/%Y:%H:%M:%S %z]", time.localtime())
    method = random.choice(http_methods)
    endpoint = random.choice(endpoints)
    protocol = 'HTTP/1.1'
    response_code = random.choice([200, 201, 202, 301, 302, 400, 401, 403, 404, 500, 503])
    content_size = random.randint(1024, 10240)

    log = {
        "host": host,
        "identd": client_identd,
        "user_id": user_id,
        "date_time": date_time,
        "method": method,
        "endpoint": endpoint,
        "protocol": protocol,
        "response_code": response_code,
        "content_size": content_size,
        "source": "web_server_logs"
    }

    return log


if __name__ == "__main__":
    cfg = read_config()

    broker_url = cfg['kafka']['broker_url']
    topic_name = cfg['kafka']['topic_name']
    compression_type = cfg['kafka']['compression_type']
    batch_size = cfg['kafka']['batch_size']
    linger_ms = cfg['kafka']['linger_ms']
    acks = cfg['kafka']['acks']

    kafka_config = {
        "bootstrap_servers": [broker_url],
        "value_serializer": lambda x: json.dumps(x).encode('utf-8'),
        "acks": acks,
        "retries": 3,
        "compression_type": compression_type,
        "max_in_flight_requests_per_connection": 5,
        "batch_size": batch_size,
        "linger_ms": linger_ms,
        "max_request_size": 1048576,
        "request_timeout_ms": 30000,
        "retry_backoff_ms": 500,
        "metadata_max_age_ms": 300000,
        "max_block_ms": 60000,
        "retry_backoff_ms": 500,
        "buffer_memory": 33554432
    }

    producer = KafkaProducer(**kafka_config)

    while True:
        log = create_log_message()

        try:
            producer.send(topic_name, value=log)
        except Exception as e:
            print(f"Error: {str(e)}")

        time.sleep(0.1)

