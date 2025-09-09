from loader import Loader
from producer import Producer
from gen_id import Generate
from dal.elasticDAL import ElasticDAL
from dal.mongoDAL import MongoDAL
from transcriber import Transcriber


def main():
    # יצירת מופע ללאודר והשמת המטאדאטה במשתנה
    loader = Loader()
    metadata = loader.get_metadata()

    # שליחת המטאדאטה לקאפקה
    producer = Producer()
    producer.publish(metadata)

    # יצירת איידיז לכל המטאדאטה
    gen = Generate()
    enriched = []
    for item in metadata:
        item["id"] = gen.generate_id(item)
        enriched.append(item)

    # שליחה לאלסטיק
    elastic = ElasticDAL()
    elastic.create_index()
    for item in enriched:
        elastic.upload_to_elastic(item)

    # שליחה למונגו
    mongo = MongoDAL()
    for item in enriched:
        mongo.upload_file(item["file_path"], item["id"])


    # תמלול הטקסט ודחיפה לאלסטיק
    transcriber = Transcriber()
    for item in enriched:
        text = transcriber.transcribe_audio(item["file_path"])
        if text:
            transcriber.update_elastic(item["id"], text)
    #print("all files have been uploaded to elastic!!!")


if __name__ == "__main__":
    main()

