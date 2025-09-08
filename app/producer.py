from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import json

load_dotenv()

class Producer:
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC")
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    def publish(self, list_metadata):
        for metadata in list_metadata:
            self.producer.send(self.topic, metadata)
        self.producer.flush()