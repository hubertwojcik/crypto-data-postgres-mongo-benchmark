from pymongo import MongoClient, ASCENDING
import time
from typing import Dict, Any, List

class MongoManager:
    def __init__(self, mongo_uri, db_name="social"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db["tweets"]
    
    def init_indexes_timed(self):
        """Inicjalizuj indeksy z pomiarem czasu."""
        print("🧱 Inicjalizacja indeksów MongoDB...")
        start_time = time.perf_counter()
        
        self.col.create_index([("date", ASCENDING)])
        self.col.create_index([("user.user_name", ASCENDING)])
        self.col.create_index([("hashtags", ASCENDING)])
        self.col.create_index([("is_retweet", ASCENDING)])
        
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
        hashtags = row_dict.get("hashtags", []) or []
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
    
    def test_read_user_stats(self, limit=20):
        """Test READ: Statystyki użytkowników z GROUP BY i agregacjami."""
        start = time.perf_counter()
        pipeline = [
            {
                "$group": {
                    "_id": "$user.user_name",
                    "tweet_count": {"$sum": 1},
                    "avg_followers": {"$avg": "$user.user_followers"},
                    "last_tweet_date": {"$max": "$date"}
                }
            },
            {
                "$match": {
                    "tweet_count": {"$gt": 0}
                }
            },
            {
                "$sort": {"tweet_count": -1}
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "user_name": "$_id",
                    "tweet_count": 1,
                    "avg_followers": 1,
                    "last_tweet_date": 1,
                    "_id": 0
                }
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_popular_hashtags(self, limit=20):
        """Test READ: Najpopularniejsze hashtagi z GROUP BY i COUNT."""
        start = time.perf_counter()
        pipeline = [
            {
                "$unwind": "$hashtags"
            },
            {
                "$group": {
                    "_id": "$hashtags",
                    "usage_count": {"$sum": 1},
                    "unique_users": {"$addToSet": "$user.user_name"}
                }
            },
            {
                "$project": {
                    "tag": "$_id",
                    "usage_count": 1,
                    "unique_users": {"$size": "$unique_users"},
                    "_id": 0
                }
            },
            {
                "$sort": {"usage_count": -1}
            },
            {
                "$limit": limit
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_daily_stats(self, days=30):
        """Test READ: Statystyki dzienne z GROUP BY daty i agregacjami."""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        start = time.perf_counter()
        pipeline = [
            {
                "$match": {
                    "date": {"$gte": cutoff_date}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$date"
                        }
                    },
                    "tweet_count": {"$sum": 1},
                    "unique_users": {"$addToSet": "$user.user_name"},
                    "retweet_count": {
                        "$sum": {
                            "$cond": [{"$eq": ["$is_retweet", True]}, 1, 0]
                        }
                    }
                }
            },
            {
                "$project": {
                    "tweet_date": "$_id",
                    "tweet_count": 1,
                    "unique_users": {"$size": "$unique_users"},
                    "retweet_count": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"tweet_date": -1}
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_aggregate_joins(self):
        """Test READ: Złożone agregacje (odpowiednik JOIN w MongoDB)."""
        start = time.perf_counter()
        pipeline = [
            {
                "$group": {
                    "_id": "$user.user_name",
                    "total_tweets": {"$sum": 1},
                    "unique_hashtags_used": {"$addToSet": "$hashtags"},
                    "avg_followers": {"$avg": "$user.user_followers"},
                    "last_activity": {"$max": "$date"}
                }
            },
            {
                "$project": {
                    "user_name": "$_id",
                    "total_tweets": 1,
                    "unique_hashtags_used": {
                        "$size": {
                            "$reduce": {
                                "input": "$unique_hashtags_used",
                                "initialValue": [],
                                "in": {"$setUnion": ["$$value", "$$this"]}
                            }
                        }
                    },
                    "avg_followers": 1,
                    "last_activity": 1,
                    "_id": 0
                }
            },
            {
                "$match": {
                    "total_tweets": {"$gte": 5}
                }
            },
            {
                "$sort": {"total_tweets": -1, "unique_hashtags_used": -1}
            },
            {
                "$limit": 50
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_user_ranking(self):
        """Test READ: Ranking użytkowników z prostym GROUP BY."""
        start = time.perf_counter()
        pipeline = [
            {
                "$group": {
                    "_id": "$user.user_name",
                    "tweet_count": {"$sum": 1},
                    "user_followers": {"$first": "$user.user_followers"},
                    "last_tweet": {"$max": "$date"}
                }
            },
            {
                "$match": {
                    "tweet_count": {"$gt": 0}
                }
            },
            {
                "$sort": {"tweet_count": -1, "user_followers": -1}
            },
            {
                "$limit": 25
            },
            {
                "$project": {
                    "user_name": "$_id",
                    "tweet_count": 1,
                    "user_followers": 1,
                    "last_tweet": 1,
                    "_id": 0
                }
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_daily_activity(self):
        """Test READ: Prosta analiza aktywności dziennej."""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=30)
        
        start = time.perf_counter()
        pipeline = [
            {
                "$match": {
                    "date": {"$gte": cutoff_date}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$date"
                        }
                    },
                    "daily_tweets": {"$sum": 1},
                    "active_users": {"$addToSet": "$user.user_name"},
                    "retweets": {
                        "$sum": {
                            "$cond": [{"$eq": ["$is_retweet", True]}, 1, 0]
                        }
                    }
                }
            },
            {
                "$project": {
                    "tweet_date": "$_id",
                    "daily_tweets": 1,
                    "active_users": {"$size": "$active_users"},
                    "retweets": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"tweet_date": -1}
            },
            {
                "$limit": 30
            }
        ]
        results = list(self.col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_hashtag_trends(self):
        """Test READ: Proste trendy hashtagów."""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=7)
        
        start = time.perf_counter()
        pipeline = [
            {
                "$match": {
                    "date": {"$gte": cutoff_date}
                }
            },
            {
                "$unwind": "$hashtags"
            },
            {
                "$group": {
                    "_id": "$hashtags",
                    "usage_count": {"$sum": 1},
                    "unique_users": {"$addToSet": "$user.user_name"},
                    "last_used": {"$max": "$date"}
                }
            },
            {
                "$match": {
                    "usage_count": {"$gte": 3}
                }
            },
            {
                "$project": {
                    "tag": "$_id",
                    "usage_count": 1,
                    "unique_users": {"$size": "$unique_users"},
                    "last_used": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"usage_count": -1}
            },
            {
                "$limit": 20
            }
        ]
        results = list(self.col.aggregate(pipeline))
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
    
    def test_create_with_existing_user(self, row_dict):
        """Test CREATE: Wstawianie tweeta z istniejącym użytkownikiem."""
        start = time.perf_counter()
        # Użyj tego samego user_name - MongoDB nie wymaga osobnej operacji
        doc = self._build_document(row_dict)
        result = self.col.insert_one(doc)
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": result.inserted_id}
    
    def test_create_with_many_hashtags(self, row_dict, num_hashtags=10):
        """Test CREATE: Wstawianie tweeta z wieloma hashtagami."""
        start = time.perf_counter()
        # Dodaj wiele hashtagów do dokumentu
        hashtags = [f"tag_{i}_{int(time.time())}" for i in range(num_hashtags)]
        row_dict_copy = row_dict.copy()
        row_dict_copy["hashtags"] = hashtags
        doc = self._build_document(row_dict_copy)
        result = self.col.insert_one(doc)
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": result.inserted_id}
    
    def test_update_user_data(self, user_name=None):
        """Test UPDATE: Aktualizacja danych użytkownika."""
        start = time.perf_counter()
        if user_name:
            result = self.col.update_many(
                {"user.user_name": user_name},
                {"$inc": {"user.user_followers": 1},
                 "$set": {"user.user_location": "Updated Location"}}
            )
        else:
            result = self.col.update_one(
                {},
                {"$inc": {"user.user_followers": 1}}
            )
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "updated_count": result.modified_count}
    
    def test_bulk_update(self, days_ago=365):
        """Test UPDATE: Masowa aktualizacja wielu tweetów."""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days_ago)
        start = time.perf_counter()
        result = self.col.update_many(
            {"date": {"$lt": cutoff_date}},
            {"$set": {"archived": True}}
        )
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "updated_count": result.modified_count}
    
    def test_delete_user_tweets(self, user_name=None):
        """Test DELETE: Usunięcie wszystkich tweetów użytkownika."""
        start = time.perf_counter()
        if user_name:
            result = self.col.delete_many({"user.user_name": user_name})
        else:
            # Usuń tweety pierwszego użytkownika
            first_user = self.col.find_one({}, {"user.user_name": 1})
            if first_user:
                result = self.col.delete_many({"user.user_name": first_user["user"]["user_name"]})
            else:
                result = type('obj', (object,), {'deleted_count': 0})()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "deleted_count": result.deleted_count}
    
    def test_delete_old_tweets(self, days_ago=365):
        """Test DELETE: Usunięcie tweetów starszych niż określona data."""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days_ago)
        start = time.perf_counter()
        result = self.col.delete_many({"date": {"$lt": cutoff_date}})
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "deleted_count": result.deleted_count}
    
    def test_batch_insert(self, rows_list, batch_size=1000):
        """Test CREATE: Batch insert wielu rekordów naraz."""
        from pymongo import InsertOne
        
        start = time.perf_counter()
        mongo_ops = []
        
        for row_dict in rows_list:
            doc = self._build_document(row_dict)
            mongo_ops.append(InsertOne(doc))
            
            if len(mongo_ops) >= batch_size:
                self.col.bulk_write(mongo_ops, ordered=False)
                mongo_ops.clear()
        
        # Final flush
        if mongo_ops:
            self.col.bulk_write(mongo_ops, ordered=False)
        
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "inserted_count": len(rows_list)}
    
    def clear_all_caches(self):
        """Wyczyść wszystkie cache - bazy danych i systemu."""
        print("\n🧹 Czyszczenie wszystkich cache...")
        total_time = 0
        
        # Wyczyść cache MongoDB
        total_time += self.clear_cache()
        
        # Wyczyść cache systemu
        total_time += self.clear_system_cache()
        
        print(f"✅ Wszystkie cache wyczyszczone w {total_time:.4f}s")
        return total_time

    def clear_cache(self):
        """Wyczyść cache MongoDB (query cache, connection pool, itp.)."""
        print("🧹 Czyszczenie cache MongoDB...")
        start_time = time.perf_counter()
        
        try:
            # Wyczyść cache planów zapytań - używamy prostszej składni
            try:
                self.db.command({"planCacheClear": 1})
            except Exception:
                pass
            
            # Wyczyść cache indeksów tylko jeśli kolekcja istnieje
            try:
                if "tweets" in self.db.list_collection_names():
                    self.db.command({"reIndex": "tweets"})
            except Exception:
                pass
            
            # Wymuś flush danych na dysk
            try:
                self.db.command({"fsync": 1})
            except Exception:
                pass
                
        except Exception as e:
            print(f"  ⚠️  Błąd podczas czyszczenia cache MongoDB: {e}")
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Cache MongoDB wyczyszczony w {elapsed:.4f}s")
        return elapsed
    
    def clear_system_cache(self):
        """Wyczyść cache systemu operacyjnego (wymaga uprawnień)."""
        print("🧹 Próba wyczyszczenia cache systemu...")
        start_time = time.perf_counter()
        
        import subprocess
        import os
        
        try:
            # Linux/macOS - wyczyść page cache, dentries i inodes
            if os.name == 'posix':
                # Synchronizuj dane na dysk
                subprocess.run(['sync'], check=False)
                
                # Próbuj wyczyścić cache (może wymagać sudo)
                try:
                    # echo 3 > /proc/sys/vm/drop_caches (Linux)
                    subprocess.run(['sudo', 'sh', '-c', 'echo 3 > /proc/sys/vm/drop_caches'], 
                                 check=False, capture_output=True)
                except Exception:
                    # Jeśli nie ma sudo lub to macOS, spróbuj purge (macOS)
                    try:
                        subprocess.run(['purge'], check=False, capture_output=True)
                    except Exception:
                        print("  ⚠️  Nie można wyczyścić cache systemu (brak uprawnień)")
        except Exception as e:
            print(f"  ⚠️  Błąd podczas czyszczenia cache systemu: {e}")
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Próba czyszczenia cache systemu zakończona w {elapsed:.4f}s")
        return elapsed

    def close(self):
        self.client.close()