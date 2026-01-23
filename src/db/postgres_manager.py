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
    
    def test_read_user_stats(self, limit=20):
        """Test READ: Statystyki użytkowników z GROUP BY i agregacjami."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    u.user_name,
                    COUNT(t.id) as tweet_count,
                    AVG(u.user_followers) as avg_followers,
                    MAX(t.date) as last_tweet_date
                FROM users u
                LEFT JOIN tweets t ON u.id = t.user_id
                GROUP BY u.id, u.user_name
                HAVING COUNT(t.id) > 0
                ORDER BY tweet_count DESC
                LIMIT %s
            """, (limit,))
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_popular_hashtags(self, limit=20):
        """Test READ: Najpopularniejsze hashtagi z GROUP BY i COUNT."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    h.tag,
                    COUNT(th.tweet_id) as usage_count,
                    COUNT(DISTINCT t.user_id) as unique_users
                FROM hashtags h
                JOIN tweet_hashtags th ON h.id = th.hashtag_id
                JOIN tweets t ON th.tweet_id = t.id
                GROUP BY h.id, h.tag
                ORDER BY usage_count DESC
                LIMIT %s
            """, (limit,))
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_daily_stats(self, days=30):
        """Test READ: Statystyki dzienne z GROUP BY daty i agregacjami."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(t.date) as tweet_date,
                    COUNT(t.id) as tweet_count,
                    COUNT(DISTINCT t.user_id) as unique_users,
                    COUNT(CASE WHEN t.is_retweet = TRUE THEN 1 END) as retweet_count
                FROM tweets t
                WHERE t.date >= NOW() - INTERVAL '%s days'
                GROUP BY DATE(t.date)
                ORDER BY tweet_date DESC
            """, (days,))
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_aggregate_joins(self):
        """Test READ: Złożone agregacje z wieloma JOINami."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    u.user_name,
                    COUNT(DISTINCT t.id) as total_tweets,
                    COUNT(DISTINCT th.hashtag_id) as unique_hashtags_used,
                    AVG(u.user_followers) as avg_followers,
                    MAX(t.date) as last_activity
                FROM users u
                JOIN tweets t ON u.id = t.user_id
                LEFT JOIN tweet_hashtags th ON t.id = th.tweet_id
                GROUP BY u.id, u.user_name
                HAVING COUNT(DISTINCT t.id) >= 5
                ORDER BY total_tweets DESC, unique_hashtags_used DESC
                LIMIT 50
            """)
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_user_ranking(self):
        """Test READ: Ranking użytkowników z prostym GROUP BY."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    u.user_name,
                    COUNT(t.id) as tweet_count,
                    u.user_followers,
                    MAX(t.date) as last_tweet
                FROM users u
                JOIN tweets t ON u.id = t.user_id
                GROUP BY u.id, u.user_name, u.user_followers
                HAVING COUNT(t.id) > 0
                ORDER BY tweet_count DESC, u.user_followers DESC
                LIMIT 25
            """)
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
    def test_read_daily_activity(self):
        """Test READ: Prosta analiza aktywności dziennej."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(t.date) as tweet_date,
                    COUNT(t.id) as daily_tweets,
                    COUNT(DISTINCT t.user_id) as active_users,
                    COUNT(CASE WHEN t.is_retweet = TRUE THEN 1 END) as retweets
                FROM tweets t
                WHERE t.date >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(t.date)
                ORDER BY tweet_date DESC
                LIMIT 30
            """)
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
    
    def test_create_with_existing_user(self, row_dict):
        """Test CREATE: Wstawianie tweeta z istniejącym użytkownikiem."""
        start = time.perf_counter()
        # Upewnij się, że użytkownik istnieje
        user_id = self.upsert_user(row_dict)
        source_id = self.get_or_create_source(row_dict.get("source"))
        # Teraz wstaw tweet dla istniejącego użytkownika
        tweet_id = self.insert_tweet(user_id, row_dict, source_id)
        self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": tweet_id}
    
    def test_create_with_many_hashtags(self, row_dict, num_hashtags=10):
        """Test CREATE: Wstawianie tweeta z wieloma hashtagami."""
        start = time.perf_counter()
        user_id = self.upsert_user(row_dict)
        source_id = self.get_or_create_source(row_dict.get("source"))
        tweet_id = self.insert_tweet(user_id, row_dict, source_id)
        
        # Dodaj wiele hashtagów
        for i in range(num_hashtags):
            tag = f"tag_{i}_{int(time.time())}"
            hashtag_id = self.get_or_create_hashtag(tag)
            self.link_tweet_hashtag(tweet_id, hashtag_id)
        
        self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "tweet_id": tweet_id}
    
    def test_update_user_data(self, user_name=None):
        """Test UPDATE: Aktualizacja danych użytkownika."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            if user_name:
                cur.execute("""
                    UPDATE users 
                    SET user_followers = user_followers + 1,
                        user_location = COALESCE(user_location, 'Updated Location')
                    WHERE user_name = %s
                """, (user_name,))
            else:
                cur.execute("""
                    UPDATE users 
                    SET user_followers = user_followers + 1
                    WHERE id = (SELECT id FROM users LIMIT 1)
                """)
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed}
    
    def test_bulk_update(self, days_ago=365):
        """Test UPDATE: Masowa aktualizacja wielu tweetów."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(f"""
                UPDATE tweets 
                SET text = text || ' [ARCHIVED]'
                WHERE date < NOW() - INTERVAL '{days_ago} days'
            """)
            updated_count = cur.rowcount
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "updated_count": updated_count}
    
    def test_delete_user_tweets(self, user_name=None):
        """Test DELETE: Usunięcie wszystkich tweetów użytkownika."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            if user_name:
                cur.execute("""
                    DELETE FROM tweets 
                    WHERE user_id IN (SELECT id FROM users WHERE user_name = %s)
                """, (user_name,))
            else:
                cur.execute("""
                    DELETE FROM tweets 
                    WHERE user_id = (SELECT id FROM users LIMIT 1)
                """)
            deleted_count = cur.rowcount
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "deleted_count": deleted_count}
    
    def test_delete_old_tweets(self, days_ago=365):
        """Test DELETE: Usunięcie tweetów starszych niż określona data."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM tweets 
                WHERE date < NOW() - INTERVAL '{days_ago} days'
            """)
            deleted_count = cur.rowcount
            self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "deleted_count": deleted_count}
    
    def test_batch_insert(self, rows_list, batch_size=1000):
        """Test CREATE: Batch insert wielu rekordów naraz."""
        start = time.perf_counter()
        
        inserted_count = 0
        batch_count = 0
        
        for i in range(0, len(rows_list), batch_size):
            batch = rows_list[i:i+batch_size]
            batch_count += 1
            
            # Przygotuj dane dla batcha
            user_data = []
            source_data = []
            tweet_data = []
            hashtag_data = []
            tweet_hashtag_data = []
            
            # Mapowanie dla szybkiego dostępu w tym batchu
            user_name_to_id = {}
            source_name_to_id = {}
            hashtag_to_id = {}
            
            # Pobierz istniejące dane
            with self.conn.cursor() as cur:
                user_names = [r.get("user_name") for r in batch if r.get("user_name")]
                if user_names:
                    cur.execute("SELECT id, user_name FROM users WHERE user_name = ANY(%s)", (user_names,))
                    for row in cur.fetchall():
                        user_name_to_id[row[1]] = row[0]
                
                source_names = [r.get("source") for r in batch if r.get("source")]
                if source_names:
                    cur.execute("SELECT id, name FROM sources WHERE name = ANY(%s)", (source_names,))
                    for row in cur.fetchall():
                        source_name_to_id[row[1]] = row[0]
            
            # Przygotuj users do wstawienia
            for row_dict in batch:
                user_name = row_dict.get("user_name")
                if user_name and user_name not in user_name_to_id:
                    user_data.append((
                        user_name,
                        row_dict.get("user_location"),
                        row_dict.get("user_description"),
                        row_dict.get("user_created"),
                        row_dict.get("user_followers"),
                        row_dict.get("user_friends"),
                        row_dict.get("user_favourites"),
                        bool(row_dict.get("user_verified")) if row_dict.get("user_verified") is not None else None
                    ))
            
            # Batch insert users
            if user_data:
                with self.conn.cursor() as cur:
                    for user_row in user_data:
                        cur.execute(INSERT_USER, user_row)
                        user_id = cur.fetchone()[0]
                        user_name_to_id[user_row[0]] = user_id
            
            # Przygotuj sources do wstawienia
            for row_dict in batch:
                source_name = row_dict.get("source")
                if source_name and source_name not in source_name_to_id:
                    source_data.append((source_name,))
            
            # Batch insert sources
            if source_data:
                with self.conn.cursor() as cur:
                    for source_row in source_data:
                        cur.execute(INSERT_SOURCE, source_row)
                        result = cur.fetchone()
                        if result:
                            source_id = result[0]
                            source_name_to_id[source_row[0]] = source_id
                        else:
                            # Source już istnieje, pobierz ID
                            cur.execute(GET_SOURCE_ID, source_row)
                            result = cur.fetchone()
                            if result:
                                source_name_to_id[source_row[0]] = result[0]
            
            # Przygotuj tweety
            for row_dict in batch:
                user_id = user_name_to_id.get(row_dict.get("user_name"))
                source_id = source_name_to_id.get(row_dict.get("source"))
                
                if user_id:
                    tweet_data.append((
                        user_id,
                        row_dict.get("date"),
                        row_dict.get("text"),
                        source_id,
                        bool(row_dict.get("is_retweet")) if row_dict.get("is_retweet") is not None else None
                    ))
            
            # Batch insert tweets (używamy execute_batch dla wydajności)
            if tweet_data:
                with self.conn.cursor() as cur:
                    execute_batch(cur, INSERT_TWEET, tweet_data, page_size=batch_size)
                    # Pobierz wstawione ID (używamy sekwencji)
                    cur.execute("SELECT last_value FROM tweets_id_seq")
                    last_seq = cur.fetchone()[0]
                    # Pobierz ostatnie wstawione tweety
                    cur.execute("SELECT id FROM tweets WHERE id > %s - %s ORDER BY id", (last_seq, len(tweet_data)))
                    inserted_tweet_ids = [row[0] for row in cur.fetchall()]
                    inserted_count += len(inserted_tweet_ids)
                    
                    # Przygotuj hashtagi i tweet_hashtags
                    for idx, row_dict in enumerate(batch):
                        if idx >= len(inserted_tweet_ids):
                            break
                        tweet_id = inserted_tweet_ids[idx]
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
                                if tag not in hashtag_to_id:
                                    hashtag_data.append((tag,))
                                    hashtag_to_id[tag] = None
                    
                    # Batch insert hashtags
                    if hashtag_data:
                        for tag_row in hashtag_data:
                            cur.execute(INSERT_HASHTAG, tag_row)
                            result = cur.fetchone()
                            if result:
                                hashtag_to_id[tag_row[0]] = result[0]
                            else:
                                cur.execute(GET_HASHTAG_ID, tag_row)
                                result = cur.fetchone()
                                if result:
                                    hashtag_to_id[tag_row[0]] = result[0]
                    
                    # Przygotuj tweet_hashtags
                    for idx, row_dict in enumerate(batch):
                        if idx >= len(inserted_tweet_ids):
                            break
                        tweet_id = inserted_tweet_ids[idx]
                        hashtags = row_dict.get("hashtags") or []
                        if isinstance(hashtags, str):
                            try:
                                import ast
                                hashtags = ast.literal_eval(hashtags)
                            except:
                                hashtags = []
                        
                        for tag in hashtags:
                            tag = str(tag).strip().lstrip("#").lower()
                            if tag and tag in hashtag_to_id and hashtag_to_id[tag]:
                                tweet_hashtag_data.append((tweet_id, hashtag_to_id[tag]))
                    
                    # Batch insert tweet_hashtags
                    if tweet_hashtag_data:
                        execute_batch(cur, INSERT_TWEET_HASHTAG, tweet_hashtag_data, page_size=batch_size)
            
            # Commit po każdym batchu
            if batch_count % 10 == 0 or i + batch_size >= len(rows_list):
                self.commit()
        
        # Final commit
        self.commit()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "inserted_count": inserted_count}

    def clear_all_caches(self):
        """Wyczyść wszystkie cache - bazy danych i systemu."""
        print("\n🧹 Czyszczenie wszystkich cache...")
        total_time = 0
        
        # Wyczyść cache PostgreSQL
        total_time += self.clear_cache()
        
        # Wyczyść cache systemu
        total_time += self.clear_system_cache()
        
        print(f"✅ Wszystkie cache wyczyszczone w {total_time:.4f}s")
        return total_time

    def clear_cache(self):
        """Wyczyść cache PostgreSQL (shared_buffers, query cache, itp.)."""
        print("🧹 Czyszczenie cache PostgreSQL...")
        start_time = time.perf_counter()
        
        try:
            # Zakończ bieżącą transakcję przed DISCARD ALL
            self.conn.commit()
            
            # Ustaw autocommit na True dla komend które nie mogą być w transakcji
            old_autocommit = self.conn.autocommit
            self.conn.autocommit = True
            
            with self.conn.cursor() as cur:
                # Wyczyść cache planów zapytań
                cur.execute("DISCARD ALL")
                
                # Wymuś checkpoint i flush bufferów
                cur.execute("CHECKPOINT")
            
            # Przywróć poprzedni tryb autocommit
            self.conn.autocommit = old_autocommit
            
            # Wykonaj pozostałe operacje w normalnej transakcji
            with self.conn.cursor() as cur:
                # Wyczyść statystyki
                cur.execute("SELECT pg_stat_reset()")
                
                # Wyczyść cache shared_buffers (wymaga uprawnień superuser)
                try:
                    cur.execute("SELECT pg_prewarm_reset()")
                except Exception:
                    # Ignoruj błąd jeśli pg_prewarm nie jest dostępne
                    pass
            
            self.conn.commit()
            
        except Exception as e:
            print(f"  ⚠️  Błąd podczas czyszczenia cache PostgreSQL: {e}")
            # Upewnij się, że autocommit jest przywrócony
            try:
                self.conn.autocommit = old_autocommit
            except:
                pass
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Cache PostgreSQL wyczyszczony w {elapsed:.4f}s")
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

    def test_read_hashtag_trends(self):
        """Test READ: Proste trendy hashtagów."""
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    h.tag,
                    COUNT(th.tweet_id) as usage_count,
                    COUNT(DISTINCT t.user_id) as unique_users,
                    MAX(t.date) as last_used
                FROM hashtags h
                JOIN tweet_hashtags th ON h.id = th.hashtag_id
                JOIN tweets t ON th.tweet_id = t.id
                WHERE t.date >= NOW() - INTERVAL '7 days'
                GROUP BY h.tag
                HAVING COUNT(th.tweet_id) >= 3
                ORDER BY usage_count DESC
                LIMIT 20
            """)
            results = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {"time": elapsed, "count": len(results)}
    
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
        self.conn.close()