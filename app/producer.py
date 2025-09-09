from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import json
from logger import Logger

load_dotenv()
logger = Logger.get_logger()

class Producer:
    try:
        def __init__(self):
            self.topic = os.getenv("KAFKA_TOPIC")
            self.producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            logger.info(f"\nProducer created with topic: {self.topic}")

    except Exception as e:
        logger.error(f"producer not created... : {e}")


    def publish(self, list_metadata):
        try:
            for metadata in list_metadata:
                self.producer.send(self.topic, metadata)
            self.producer.flush()
            logger.info(f"\nall Metadata sent to Topic: {self.topic}")
            logger.info("submission completed successfully!\n")

        except Exception as e:
            logger.error(f"Failed to send metadata: {e}")

