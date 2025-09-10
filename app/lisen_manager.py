from consumer import Consumer
from utils.logger import Logger

logger = Logger.get_logger()

def run_consumer():
    logger.info("Listening has begun...")
    try:
        consumer = Consumer()
        consumer.listen()
        logger.info("Listening ended successfully!")
    except Exception as e:
        logger.error(f"Listening failed: {e}")


if __name__ == "__main__":
    run_consumer()
