import pathlib
import os
from dotenv import load_dotenv
import datetime
from logger import Logger

load_dotenv()
logger = Logger.get_logger()

class Loader:
    def __init__(self):
        self.path_env = os.getenv("PATH_FILE")
        self.path = pathlib.Path(self.path_env)
        self.all_path = [item for item in self.path.iterdir() if item.is_file()]
        logger.info(f"Loading files from a path: {self.path_env}...")
        logger.info("Loading completed successfully!\n")

    def get_metadata(self):
        list_metadata = []
        for file in self.all_path:
            try:
                metadata = {
                    "file_name": file.name,
                    "file_path": str(file.resolve()),
                    "file_size_bytes": file.stat().st_size,
                    "created_at": datetime.datetime.fromtimestamp(file.stat().st_ctime).isoformat()}

                list_metadata.append(metadata)
                logger.info(f"Metadata extracted for file: {file.name}")

            except Exception as e:
                logger.error(f"Failed to extract metadata from {file.name}: {e}")
        return list_metadata
