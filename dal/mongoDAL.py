from pymongo import MongoClient
from gridfs import GridFS
import os
from dotenv import load_dotenv


load_dotenv()

class MongoDAL:
    def __init__(self):
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        db_name = os.getenv("DB_NAME", "muezzin")
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.fs = GridFS(self.db)


    def upload_file(self,file_path, file_id):
        with open(file_path, "rb") as f:
            file_id = self.fs.put(f, _id= file_id, type=".wav")
        return file_id