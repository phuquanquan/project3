import os

DATABASE_URL = os.environ.get('DATABASE_URL')
SECRET_KEY = os.environ.get('SECRET_KEY')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.environ.get('KAFKA_TOPIC_NAME')
HDFS_URL = os.environ.get('HDFS_URL')
