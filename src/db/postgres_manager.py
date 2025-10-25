import psycopg2
from psycopg2.extras import execture_batch

DDL = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  user_name TEXT UNIQUE,
  user_location TEXT,
  user_description TEXT,
  user_created TIMESTAMP,
  user_followers BIGINT,
  user_friends BIGINT,
  user_favourites BIGINT,
  user_verified BOOLEAN
);

CREATE TABLE IF NOT EXISTS sources (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS tweets (
  id BIGSERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id),
  date TIMESTAMP,
  text TEXT,
  source_id INT REFERENCES sources(id),
  is_retweet BOOLEAN
);

CREATE TABLE IF NOT EXISTS hashtags (
  id SERIAL PRIMARY KEY,
  tag TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS tweet_hashtags (
  tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
  hashtag_id INT REFERENCES hashtags(id) ON DELETE CASCADE,
  PRIMARY KEY (tweet_id, hashtag_id)
);

CREATE INDEX IF NOT EXISTS idx_tweets_user_date ON tweets(user_id, date);
CREATE INDEX IF NOT EXISTS idx_hashtag_tag ON hashtags(tag);
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