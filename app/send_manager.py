from loader import Loader
from producer import Producer
from processor import Processor

def main():
    # יצירת מופע ללאודר והשמת המטאדאטה במשתנה
    loader = Loader()
    metadata = loader.get_metadata()

    # שליחת המטאדאטה לקאפקה
    print("send massages to kafka... ")
    producer = Producer()
    producer.publish(metadata)
    print("all items sent to kafka!")

    # יצירת איידיז לכל המטאדאטה
    processor = Processor()
    enriched = []

    for item in metadata:
        item["id"] = processor.generate_id(item)
        enriched.append(item)

    for item in enriched:
        print(item)

if __name__ == "__main__":
    main()

