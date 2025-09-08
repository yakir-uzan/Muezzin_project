import json
import pathlib
import os
from dotenv import load_dotenv
import datetime

load_dotenv()

class Loader:
    def __init__(self):
        self.path_env = os.getenv("PATH_FILE")
        self.path = pathlib.Path(self.path_env)
        self.all_path = [item for item in self.path.iterdir() if item.is_file()]

    def get_metadata(self):
        list_metadata = []
        for file in self.all_path:
            metadata = {
                "file_name": file.name,
                "file_path": str(file.resolve()),
                "file_size_bytes": file.stat().st_size,
                "created_at": datetime.datetime.fromtimestamp(file.stat().st_ctime).isoformat()
            }
            list_metadata.append(metadata)
        return list_metadata


l = Loader()
l.get_metadata()
