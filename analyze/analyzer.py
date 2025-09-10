from dal.elasticDAL import ElasticDAL
import base64
from utils.logger import Logger

logger = Logger.get_logger()

class AnalyzeText:
    def __init__(self):
        self.elastic = ElasticDAL()
        self.index = self.elastic.index

        try:
            # סט של 2 רשימות המילים המוצפנות אחרי פיענוח
            self.hostile = set(self.convert_text("R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT"))
            self.less_hostile = set(self.convert_text("RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ=="))
            logger.info("All words deciphered!")
        except Exception as e:
            logger.error(f"Decryption failed... : {e}")

    #  פיענוח מילים מןצפנות לרשימה
    def convert_text(self, encrypt_text):
        decoded_bytes = base64.b64decode(encrypt_text)
        decoded_str = decoded_bytes.decode('utf-8')
        return [word.strip().lower() for word in decoded_str.split(",")]

    # משיכת כל הקבצים המתומללים מאלסטיק
    def get_text_from_elastic(self, field, size):
        try:
            response = self.elastic.client.search(index=self.index, body={"size": size, "_source": [field], "query": {"match_all": {}}})
            return [(hit["_id"],hit["_source"][field]) for hit in response["hits"]["hits"]]
        except Exception as e:
            logger.error(f"File download failed: {e}")


    # פונקציית עזר: ניתוח טקסט בודד
    def analyze_text(self, text):
        words = text.lower().split()
        score = 0

        # בדיקה אם יש בטקסט מילים עויינות או פחות
        for i in range(len(words)):
            if words[i] in self.hostile:
                score += 2
            elif words[i] in self.less_hostile:
                score += 1

            # בדיקת צמדי מילים
            if i < len(words) - 1:
                double = words[i] + " " + words[i + 1]
                if double in self.hostile:
                    score += 2
                elif double in self.less_hostile:
                    score += 1
            per_score = round(score / len(words) * 100,2) if len(words) > 0 else 0
            return per_score

    def get_level(self, per_score):
        if per_score >= 10:
            return "Hostile"
        elif per_score >= 2:
            return "Less Hostile"
        else:
            return "Neutral"

    def analyze_all(self, field, size):
        all_texts = self.get_text_from_elastic(field, size)
        results = []
        for doc_id, text in all_texts:
            score_percent = self.analyze_text(text)
            is_bds = score_percent >= 10
            threat = self.get_level(score_percent)

            enriched_doc = {
                "id": doc_id,
                "transcription": text,
                "bds_percent": score_percent,
                "is_bds": is_bds,
                "bds_threat_level": threat}

            results.append(enriched_doc)
        return results

    def upload_to_elastic(self, field, size):
        results = self.analyze_all(field, size)
        for doc in results:
            self.elastic.upload_to_elastic(doc)

