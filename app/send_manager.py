from loader import Loader
from producer import Producer
from processor import Processor
from dal.elasticDAL import ElasticDAL
from dal.mongoDAL import MongoDAL


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

    # for item in enriched:
    #     print(item)

    # שליחה לאלסטיק
    elastic = ElasticDAL()
    for item in enriched:
        elastic.upload(item)
    print("upload all items to elastic")

    # שליחה למונגו
    mongo = MongoDAL()
    for item in enriched:
        mongo.upload_file(item["file_path"], item["id"])
    print("upload all items to mongoDB")


if __name__ == "__main__":
    main()

