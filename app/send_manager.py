from loader import Loader
from producer import Producer

def main():
    loader = Loader()
    metadata = loader.get_metadata()

    print("send massages to kafka... ")
    producer = Producer()
    producer.publish(metadata)
    print("all items sent to kafka!")

if __name__ == "__main__":
    main()

