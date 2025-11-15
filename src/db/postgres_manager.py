from os import curdir
import psycopg2
import time
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
    
    def init_schema_timed(self):
        """Inicjalizuj schemat z pomiarem czasu."""
        print("🧱 Inicjalizacja schematu PostgreSQL...")
        start_time = time.perf_counter()
        
        self.init_schema()
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Schemat PostgreSQL zainicjalizowany w {elapsed:.4f}s")
        return elapsed
    
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
            source = cur.fetchone()
            if source:
                return source[0]
            cur.execute(GET_SOURCE_ID, (name,))
            return cur.fetchone()[0]

    def insert_tweet(self, user_id, row, source_id):
        with self.conn.cursor() as cur:
            cur.execute(INSERT_TWEET, (
                user_id,
                row["date"],
                row["text"],
                source_id,
                bool(row.get('is_retweet')) if row.get('is_retweet') is not None else None                
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
            hashtag = cur.fetchone()
            if hashtag:
                return hashtag[0]
            cur.execute(GET_HASHTAG_ID, (tag,))
            return cur.fetchone()[0]

    def link_tweet_hashtag(self, tweet_id, hashtag_id):
        with self.conn.cursor() as cur:
            cur.execute(INSERT_TWEET_HASHTAG, (tweet_id, hashtag_id))

    def commit(self):
        self.conn.commit()

    def clear_database(self):
        """Wyczyść wszystkie tabele w bazie danych."""
        print("🧹 Czyszczenie bazy PostgreSQL...")
        start_time = time.perf_counter()
        
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS tweet_hashtags CASCADE")
            cur.execute("DROP TABLE IF EXISTS tweets CASCADE")
            cur.execute("DROP TABLE IF EXISTS hashtags CASCADE")
            cur.execute("DROP TABLE IF EXISTS sources CASCADE")
            cur.execute("DROP TABLE IF EXISTS users CASCADE")
        self.conn.commit()
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ PostgreSQL wyczyszczone w {elapsed:.4f}s")
        return elapsed

    def load_data_from_dataframe(self, df, batch_size=1000):
        """Ładuje dane z DataFrame do PostgreSQL z pomiarem czasu."""
        print(f"📥 Ładowanie {len(df):,} rekordów do PostgreSQL...")
        start_time = time.perf_counter()
        
        # Inicjalizuj schemat
        self.init_schema()
        
        total = 0
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Upsert user
            user_id = self.upsert_user(row_dict)
            
            # Get or create source
            source_id = self.get_or_create_source(row_dict.get("source"))
            
            # Insert tweet
            tweet_id = self.insert_tweet(user_id, row_dict, source_id)
            
            # Process hashtags
            hashtags = row_dict.get("hashtags") or []
            if isinstance(hashtags, str):
                try:
                    import ast
                    hashtags = ast.literal_eval(hashtags)
                except:
                    hashtags = []
            
            for tag in hashtags:
                tag = str(tag).strip().lstrip("#").lower()
                if tag:
                    hashtag_id = self.get_or_create_hashtag(tag)
                    self.link_tweet_hashtag(tweet_id, hashtag_id)
            
            total += 1
            if total % batch_size == 0:
                self.commit()
        
        # Final commit
        self.commit()
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Załadowano {total:,} rekordów do PostgreSQL w {elapsed:.4f}s")
        return elapsed
    
    def test_read_count(self):
        """Test READ: Liczenie rekordów."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tweets")
            count = cur.fetchone()[0]
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": count}
    
    def test_read_recent(self, limit=100):
        """Test READ: Pobieranie najnowszych tweetów."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT t.text, u.user_name, t.date 
                FROM tweets t 
                JOIN users u ON t.user_id = u.id 
                ORDER BY t.date DESC 
                LIMIT %s
            """, (limit,))
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_hashtag(self, hashtag="bitcoin", limit=50):
        """Test READ: Wyszukiwanie po hashtagach."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT t.text, u.user_name, h.tag
                FROM tweets t
                JOIN users u ON t.user_id = u.id
                JOIN tweet_hashtags th ON t.id = th.tweet_id
                JOIN hashtags h ON th.hashtag_id = h.id
                WHERE h.tag = %s
                ORDER BY t.date DESC
                LIMIT %s
            """, (hashtag, limit))
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_create(self, row_dict):
        """Test CREATE: Wstawianie nowego rekordu."""
        start = time.perf_counter()
        user_id = self.upsert_user(row_dict)
        source_id = self.get_or_create_source(row_dict.get("source"))
        tweet_id = self.insert_tweet(user_id, row_dict, source_id)
        self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": tweet_id}
    
    def test_update(self, tweet_id=None):
        """Test UPDATE: Aktualizacja rekordu."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            if tweet_id:
                cur.execute("UPDATE tweets SET text = %s WHERE id = %s", 
                           (f"Updated text {time.time()}", tweet_id))
            else:
                cur.execute("UPDATE tweets SET text = text || ' [UPDATED]' WHERE id = (SELECT id FROM tweets LIMIT 1)")
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed}
    
    def test_delete(self, tweet_id=None):
        """Test DELETE: Usuwanie rekordu."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            if tweet_id:
                cur.execute("DELETE FROM tweets WHERE id = %s", (tweet_id,))
            else:
                cur.execute("DELETE FROM tweets WHERE id = (SELECT id FROM tweets LIMIT 1)")
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed}

    def close(self):
        self.conn.close()