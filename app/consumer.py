from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

class Consumer:
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")))


c = Consumer()







