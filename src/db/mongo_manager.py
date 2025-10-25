from pymongo import MongoClient, ASCENDING

class MongoManager:
    def __init__(self, mongo_uri, db_name="social"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db["tweets"]
    
    def init_schema(self):
        self.col.create_index([("date", ASCENDING)])
        self.col.create_index([("user.user_name", ASCENDING)])
        self.col.create_index([("hashtags", ASCENDING)])
        self.col.create_index([("is_retweet", ASCENDING)])
    
    def insert_tweet_document(self, doc):
        return self.col.insert_one(doc)
    
    def insert_tweet_document(self, doc):
        return self.col.insert_one(doc)
    
    def close(self):
        self.client.close()

      