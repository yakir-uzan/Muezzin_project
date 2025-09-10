from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from utils.logger import Logger

load_dotenv()
logger = Logger.get_logger()

class ElasticDAL:
    def __init__(self):
        try:
            logger.info("Creates connection with elastic...")
            self.index = os.getenv("ELASTIC_INDEX")
            self.client = Elasticsearch(
                os.getenv("ELASTIC_HOST"),
                basic_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD")),
                verify_certs=False)
            logger.info(f"connected to elastic in address {os.getenv("ELASTIC_HOST")} successful!\n")

        except Exception as e:
            logger.error(f"Connection to Elastic failed... : {e}")


    def create_index(self):
        mapping = {"mappings": {"properties":
                    {"id": {"type": "keyword"},
                    "file_name": {"type": "text"},
                    "file_path": {"type": "text"},
                    "file_size_bytes": {"type": "long"},
                    "created_at": {"type": "date"},
                    "transcriber": {"type": "text"}}}}
        try:
            if self.client.indices.exists(index=self.index):
                self.client.indices.delete(index=self.index)
                self.client.indices.create(index=self.index, body=mapping)
                logger.info(f"Index {self.index} creation completed!\n")

        except Exception as e:
            logger.error(f"Index {self.index} creation failed... {e}")


    def upload_to_elastic(self, doc):
        try:
            self.client.index(index=self.index, id=doc["id"], document=doc)
            logger.info(f"Upload to elastic file with id: {doc['id']}")
        except Exception as e:
            logger.error(f"\nUpload to Elastic failed... : {e}")
