from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()

class ElasticDAL:
    def __init__(self):
        self.index = os.getenv("ELASTIC_INDEX", "muezzin")
        self.client = Elasticsearch(
            os.getenv("ELASTIC_HOST", "http://localhost:9200"),
            basic_auth=(
                os.getenv("ELASTIC_USER", "elastic"),
                os.getenv("ELASTIC_PASSWORD", "pass")),
            verify_certs=False)

        if not self.client.indices.exists(index=self.index):
            self.create_index()

    def create_index(self):
        mapping = {"mappings": {"properties": {
                    "id": {"type": "keyword"},
                    "file_name": {"type": "text"},
                    "file_path": {"type": "text"},
                    "file_size_bytes": {"type": "long"},
                    "created_at": {"type": "date"}}}}

        self.client.indices.create(index=self.index, body=mapping)

    def upload(self, doc):
        self.client.index(index=self.index, id=doc["id"], document=doc)