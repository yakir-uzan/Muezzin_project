import speech_recognition as sr
from dal.elasticDAL import ElasticDAL
from logger import Logger

logger = Logger.get_logger()

class Transcriber:
    def __init__(self):
        self.elastic = ElasticDAL()
        self.recognizer = sr.Recognizer()

    def transcribe_audio(self, file_path):
        try:
            with sr.AudioFile(file_path) as source:
                audio = self.recognizer.record(source)
            text = self.recognizer.recognize_google(audio)
            logger.info(f"Transcription to file : {file_path} performed!")
            return text

        except Exception as e:
            logger.error(f"Transcription failed for {file_path}: {e}")
            return ""


    def update_elastic(self, doc_id, transcription):
        try:
            self.elastic.client.update(index=self.elastic.index, id=doc_id, body={"doc": {"transcription": transcription}})
            logger.info(f"Updated transcription in Elastic for id: {doc_id}")
            logger.info("Updated all text to Elastic")

        except Exception as e:
            logger.error(f"Failed to update Elastic document {doc_id}: {e}")


