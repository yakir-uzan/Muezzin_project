from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import json
from loader import Loader

load_dotenv()

class Producer:
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC")
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    def publish(self, message):
        self.producer.send(self.topic, message)
        self.producer.flush()


p = Producer()
p.publish(Loader().get_metadata())