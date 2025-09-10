from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
from utils.logger import Logger

load_dotenv()
logger = Logger.get_logger()

class Consumer:
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        logger.info(f"Connecting to Kafka on topic: {self.topic}, servers: {self.bootstrap_servers}")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id='podcast_consumer_group')


    def listen(self):
        try:
            logger.info("consumer started listening...")
            for message in self.consumer:
                logger.info("Received message from Kafka")
                #print(json.dumps(message.value, indent=4))
        except Exception as e:
            logger.error(f"consumer failed to listening: {e}")

