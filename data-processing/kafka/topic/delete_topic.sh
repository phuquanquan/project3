#!/bin/bash

# Load config from config.yml
eval $(parse_yaml config.yml "config_")

# Delete Kafka topic
kafka-topics --delete --topic "${config_kafka_topic_name}" --bootstrap-server "${config_kafka_broker_host}:${config_kafka_broker_port}"
