from kafka import KafkaConsumer

def read_from_kafka(broker_url, topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[broker_url],
        value_deserializer=lambda m: m.decode('ascii')
    )
    for message in consumer:
        yield message.value
