from pymongo import MongoClient
from gridfs import GridFS
import os
from dotenv import load_dotenv
from utils.logger import Logger

load_dotenv()
logger = Logger.get_logger()

class MongoDAL:
    def __init__(self):
        try:
            uri = os.getenv("MONGO_URI")
            db_name = os.getenv("DB_NAME")
            self.client = MongoClient(uri)
            self.db = self.client[db_name]
            logger.info(f"\nconnection to mongoDB with uri: {uri}, DB name is: {db_name}\n")
            self.fs = GridFS(self.db)

        except Exception as e:
            logger.error(f"\nconnection to mongoDB failed: {e}\n")


    def upload_file(self,file_path, file_id):
        try:
            with open(file_path, "rb") as f:
                file_id = self.fs.put(f, id_gen= file_id, type=".wav")
                logger.info(f"file stored in mongoDB with id: {file_id}")
            return file_id

        except Exception as e:
            logger.error(f"failed to store file... : {e}")
            return None

