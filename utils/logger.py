import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()

class Logger:
    _logger = None

    @classmethod
    def get_logger(cls, name= os.getenv("NAME_LOG"),
                        es_host= os.getenv("ES_HOST_LOG"),
                        index= os.getenv("INDEX_LOG"),
                        level=logging.DEBUG):
        if cls._logger:
            return cls._logger

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            es = Elasticsearch(es_host)

            class ESHandler(logging.Handler):
                def emit(self, record):
                    try:
                        es.index(index=index, document={
                            "timestamp": datetime.utcnow().isoformat(),
                            "level": record.levelname,
                            "logger": record.name,
                            "message": record.getMessage()
                        })
                    except Exception as e:
                        print(f"[Logger] Failed to log to Elasticsearch: {e}")


            logger.addHandler(ESHandler())
            logger.addHandler(logging.StreamHandler())

        cls._logger = logger
        return logger
