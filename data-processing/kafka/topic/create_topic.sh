#!/bin/bash

# Load config from config.yml
eval $(parse_yaml config.yml "config_")

# Create Kafka topic
kafka-topics --create --topic "${config_kafka_topic_name}" --partitions 1 --replication-factor 1 --bootstrap-server "${config_kafka_broker_host}:${config_kafka_broker_port}"
