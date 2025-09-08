import hashlib
from loader import Loader

class Processor:
    @staticmethod
    def generate_id(metadata):
        json_str = str(sorted(metadata.items()))
        return hashlib.sha256(json_str.encode()).hexdigest()


