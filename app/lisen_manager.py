from consumer import Consumer
from logger import Logger

logger = Logger.get_logger()

def main():
    logger.info("Listening has begun...")
    try:
        consumer = Consumer()
        consumer.listen()
        logger.info("Listening ended successfully!")
    except Exception as e:
        logger.error(f"Listening failed: {e}")


if __name__ == "__main__":
    main()
