from pymongo import MongoClient, ASCENDING
import time
from typing import Dict, Any, List

class MongoManager:
    def __init__(self, mongo_uri, db_name="social"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db["tweets"]
    
    def init_indexes(self):
        """Create indexes for better query performance"""
        self.col.create_index([("date", ASCENDING)])
        self.col.create_index([("user.user_name", ASCENDING)])
        self.col.create_index([("hashtags", ASCENDING)])
        self.col.create_index([("is_retweet", ASCENDING)])
    
    def init_indexes_timed(self):
        """Inicjalizuj indeksy z pomiarem czasu."""
        print("🧱 Inicjalizacja indeksów MongoDB...")
        start_time = time.perf_counter()
        
        self.init_indexes()
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Indeksy MongoDB zainicjalizowane w {elapsed:.4f}s")
        return elapsed
    
    def insert_tweet_document(self, doc):
        return self.col.insert_one(doc)
    
    def clear_database(self):
        """Wyczyść kolekcję w bazie danych."""
        print("🧹 Czyszczenie bazy MongoDB...")
        start_time = time.perf_counter()
        
        self.col.drop()
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ MongoDB wyczyszczone w {elapsed:.4f}s")
        return elapsed
    
    def _build_document(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Buduje dokument MongoDB z wiersza DataFrame."""
        hashtags = row_dict.get("hashtags") or []
        if isinstance(hashtags, str):
            try:
                import ast
                hashtags = ast.literal_eval(hashtags)
            except:
                hashtags = []
        hashtags = [str(h).strip().lstrip("#").lower() for h in hashtags if str(h).strip()]
        
        return {
            "user": {
                "user_name": row_dict.get("user_name"),
                "user_location": row_dict.get("user_location"),
                "user_description": row_dict.get("user_description"),
                "user_created": row_dict.get("user_created"),
                "user_followers": row_dict.get("user_followers"),
                "user_friends": row_dict.get("user_friends"),
                "user_favourites": row_dict.get("user_favourites"),
                "user_verified": bool(row_dict.get("user_verified")) if row_dict.get("user_verified") is not None else False,
            },
            "date": row_dict.get("date"),
            "text": row_dict.get("text"),
            "hashtags": hashtags,
            "source": row_dict.get("source") or None,
            "is_retweet": bool(row_dict.get("is_retweet")) if row_dict.get("is_retweet") is not None else False,
        }
    
    def load_data_from_dataframe(self, df, batch_size=1000):
        """Ładuje dane z DataFrame do MongoDB z pomiarem czasu."""
        print(f"📥 Ładowanie {len(df):,} rekordów do MongoDB...")
        start_time = time.perf_counter()
        
        # Inicjalizuj indeksy
        self.init_indexes()
        
        from pymongo import InsertOne
        mongo_ops: List[InsertOne] = []
        total = 0
        
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            doc = self._build_document(row_dict)
            mongo_ops.append(InsertOne(doc))
            
            total += 1
            if total % batch_size == 0:
                self.col.bulk_write(mongo_ops, ordered=False)
                mongo_ops.clear()
        
        # Final flush
        if mongo_ops:
            self.col.bulk_write(mongo_ops, ordered=False)
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Załadowano {total:,} rekordów do MongoDB w {elapsed:.4f}s")
        return elapsed
    
    def test_read_count(self):
        """Test READ: Liczenie rekordów."""
        start = time.perf_counter()
        count = self.col.count_documents({})
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": count}
    
    def test_read_recent(self, limit=100):
        """Test READ: Pobieranie najnowszych tweetów."""
        start = time.perf_counter()
        results = list(self.col.find(
            {},
            {"text": 1, "user.user_name": 1, "date": 1}
        ).sort("date", -1).limit(limit))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_hashtag(self, hashtag="bitcoin", limit=50):
        """Test READ: Wyszukiwanie po hashtagach."""
        start = time.perf_counter()
        results = list(self.col.find(
            {"hashtags": hashtag},
            {"text": 1, "user.user_name": 1, "hashtags": 1}
        ).sort("date", -1).limit(limit))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_create(self, row_dict):
        """Test CREATE: Wstawianie nowego rekordu."""
        start = time.perf_counter()
        doc = self._build_document(row_dict)
        result = self.col.insert_one(doc)
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": result.inserted_id}
    
    def test_update(self, tweet_id=None):
        """Test UPDATE: Aktualizacja rekordu."""
        start = time.perf_counter()
        if tweet_id:
            self.col.update_one({"_id": tweet_id}, {"$set": {"text": f"Updated text {time.time()}"}})
        else:
            self.col.update_one({}, {"$set": {"text": "Updated text"}})
        elapsed = time.perf_counter() - start
        return {"time": elapsed}
    
    def test_delete(self, tweet_id=None):
        """Test DELETE: Usuwanie rekordu."""
        start = time.perf_counter()
        if tweet_id:
            self.col.delete_one({"_id": tweet_id})
        else:
            self.col.delete_one({})
        elapsed = time.perf_counter() - start
        return {"time": elapsed}

    def close(self):
        self.client.close()

      