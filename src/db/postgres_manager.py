from os import curdir
import psycopg2
from psycopg2.extras import execute_batch

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

INSERT_USER = """
INSERT INTO users (user_name, user_location, user_description, user_created,
                   user_followers, user_friends, user_favourites, user_verified)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (user_name) DO UPDATE
SET user_location = EXCLUDED.user_location,
    user_description = EXCLUDED.user_description,
    user_created = EXCLUDED.user_created,
    user_followers = EXCLUDED.user_followers,
    user_friends = EXCLUDED.user_friends,
    user_favourites = EXCLUDED.user_favourites,
    user_verified = EXCLUDED.user_verified
RETURNING id;
"""

INSERT_SOURCE = """
INSERT INTO sources(name) VALUES(%s)
ON CONFLICT (name) DO NOTHING
RETURNING id;
"""

GET_SOURCE_ID = "SELECT id FROM sources WHERE name=%s"
GET_USER_ID = "SELECT id FROM users WHERE user_name=%s"
INSERT_TWEET = """
INSERT INTO tweets(user_id, date, text, source_id, is_retweet)
VALUES (%s,%s,%s,%s,%s)
RETURNING id;
"""

INSERT_HASHTAG = """
INSERT INTO hashtags(tag) VALUES(%s)
ON CONFLICT (tag) DO NOTHING
RETURNING id;
"""
GET_HASHTAG_ID = "SELECT id FROM hashtags WHERE tag=%s"
INSERT_TWEET_HASHTAG = """
INSERT INTO tweet_hashtags(tweet_id, hashtag_id)
VALUES (%s,%s) ON CONFLICT DO NOTHING;
"""

class PostgresManager:
    def __init__(self, host, port, db, user, password):
        self.conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        self.conn.autocommit = False
    
    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(DDL)
        self.conn.commit()
    
    def upsert_user(self, row):
        with self.conn.cursor() as cur:
            cur.execute(INSERT_USER, (
                row["user_name"],
                row.get("user_location"),
                row.get("user_description"),
                row.get("user_created"),
                row.get("user_followers"),
                row.get("user_friends"),
                row.get("user_favourites"),
                bool(row.get("user_verified")) if row.get("user_verified") is not None else None
            ))
            uid = cur.fetchone()[0]
        self.conn.commit()
        return uid

    def get_or_create_source(self, name):
        if not name:
            return None
        with self.conn.cursor() as cur:
            cur.execute(GET_SOURCE_ID, (name,))
            source_id = cur.fetchone()
            if source_id:
                return source_id[0]
            cur.execute(INSERT_SOURCE, (name,))
            source = source.fetchone()
            if source:
                return source[0]
            cur.execute(GET_SOURCE_ID, (name,))
            return cur.fetchone()[0]

    def insert_tweet(self, row):
        with self.conn.cursor() as cur:
            cur.execute(INSERT_TWEET, (
                row["user_id"],
                row["date"],
                row["text"],
                self.get_or_create_source(row["source"]),
                bool(row['is_retweet']) if row['is_retweet'] is not None else None                
            ))
        tweet_id = cur.fetchone()[0]        
        return tweet_id
    
    def get_or_create_hashtag(self, tag):        
        with self.conn.cursor() as cur:
            cur.execute(GET_HASHTAG_ID, (tag,))
            hashtag_id = cur.fetchone()
            if hashtag_id:
                return hashtag_id[0]
            cur.execute(INSERT_HASHTAG, (tag,))
            hastag = cur.fetchone()
            if hastag:
                return hastag[0]
            cur.execute(GET_HASHTAG_ID, (tag,))
            return cur.fetchone()[0]

    def link_tweet_hashtag(self, tweet_id, hashtag_id):
        with self.conn.cursor() as cur:
            cur.execute(INSERT_TWEET_HASHTAG, (tweet_id, hashtag_id))

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()