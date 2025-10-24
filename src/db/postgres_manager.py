import psycopg2
from psycopg2.extras import execture_batch

DDL = """

"""

class PostgresManager:
    def __init__(self, host, port, db, user, password):
        pass
    
    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(DDL)
        self.conn.commit()
    
    def upsert_user(self, row):
        pass

    def get_or_create_source(self, name):
        pass
    
    def insert_tweet(self, row):
        pass
    
    def get_or_create_hashtag(self, name):
        pass
    
    def link_tweet_hashtag(self, tweet_id, hashtag_id):
        pass
    
    def commit(self):
        pass

    def close(self):
        pass