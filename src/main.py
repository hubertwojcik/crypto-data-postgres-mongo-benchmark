import time
import os
from typing import List, Dict, Any

from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager
from src.etl.load_tweets import load_csv

from pymongo import InsertOne

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

def build_mongo_doc(row: Dict[str, Any]) -> Dict[str, Any]:
    """Buduje dokument dla jednej KOLEKCJI 'tweets' (zagnieżdżony user)."""
    hashtags = row.get("hashtags") or []
    hashtags = [str(h).strip().lstrip("#").lower() for h in hashtags if str(h).strip()]
    doc = {
        "user": {
            "user_name": row.get("user_name"),
            "user_location": row.get("user_location"),
            "user_description": row.get("user_description"),
            "user_created": row.get("user_created"),
            "user_followers": row.get("user_followers"),
            "user_friends": row.get("user_friends"),
            "user_favourites": row.get("user_favourites"),
            "user_verified": bool(row.get("user_verified")) if row.get("user_verified") is not None else False,
        },
        "date": row.get("date"),
        "text": row.get("text"),
        "hashtags": hashtags,
        "source": row.get("source") or None,
        "is_retweet": bool(row.get("is_retweet")) if row.get("is_retweet") is not None else False,
    }
    return doc

def flush_postgres(pg: PostgresManager):
    """Commit dla Postgresa (dla spójności logów)."""
    pg.commit()

def flush_mongo(mg: MongoManager, ops: List[InsertOne]) -> int:
    """Wysyła zebrane operacje do Mongo w jednym `bulk_write`."""
    if not ops:
        return 0
    res = mg.col.bulk_write(ops, ordered=False)
    return res.inserted_count

def analyze_csv_data(csv_path: str):
    """Przeanalizuj dane CSV i wyświetl statystyki."""
    print(f"📊 Analiza danych z pliku: {csv_path}")
    
    import pandas as pd
    
    # Wczytaj dane do pandas DataFrame
    df = pd.read_csv(csv_path)
    
    print(f"\n📈 Podstawowe statystyki:")
    print(f"  Liczba rekordów: {len(df):,}")
    print(f"  Liczba kolumn: {len(df.columns)}")
    print(f"  Rozmiar pliku: {os.path.getsize(csv_path) / (1024*1024):.1f} MB")
    
    print(f"\n🔍 Analiza kolumn:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        unique_count = df[col].nunique()
        
        print(f"  {col}:")
        print(f"    Brakujące wartości: {null_count:,} ({null_pct:.1f}%)")
        print(f"    Unikalne wartości: {unique_count:,}")
        
        # Pokaż przykładowe wartości dla pierwszych 5 kolumn
        if col in df.columns[:5]:
            sample_values = df[col].dropna().head(3).tolist()
            print(f"    Przykłady: {sample_values}")
    
    return df


def clean_csv_data(df):
    """Wyczyść i przygotuj dane CSV."""
    print(f"\n🧹 Czyszczenie danych...")
    
    original_count = len(df)
    
    # 1. Usuń duplikaty
    df_cleaned = df.drop_duplicates()
    duplicates_removed = original_count - len(df_cleaned)
    print(f"  Usunięto duplikatów: {duplicates_removed:,}")
    
    # 2. Sprawdź i wyczyść kolumny tekstowe
    text_columns = ['user_name', 'user_location', 'user_description', 'text']
    for col in text_columns:
        if col in df_cleaned.columns:
            # Usuń rekordy z pustymi stringami
            df_cleaned = df_cleaned[df_cleaned[col].fillna('').str.strip() != '']
            print(f"  Wyczyściono kolumnę {col}")
    
    # 3. Sprawdź user_name - musi być unikalny i niepusty
    df_cleaned = df_cleaned.dropna(subset=['user_name'])
    df_cleaned = df_cleaned[df_cleaned['user_name'].str.strip() != '']
    
    # 4. Sprawdź date - musi być prawidłowa data
    df_cleaned = df_cleaned.dropna(subset=['date'])
    
    # 5. Sprawdź text - musi być niepusty
    df_cleaned = df_cleaned.dropna(subset=['text'])
    df_cleaned = df_cleaned[df_cleaned['text'].str.strip() != '']
    
    # 6. Wyczyść hashtags - usuń puste listy i nieprawidłowe formaty
    if 'hashtags' in df_cleaned.columns:
        # Zastąp NaN pustymi listami
        df_cleaned['hashtags'] = df_cleaned['hashtags'].fillna('[]')
        # Usuń rekordy z pustymi listami hashtagów (opcjonalnie)
        # df_cleaned = df_cleaned[df_cleaned['hashtags'] != '[]']
    
    # 7. Wyczyść kolumny numeryczne
    numeric_columns = ['user_followers', 'user_friends', 'user_favourites']
    for col in numeric_columns:
        if col in df_cleaned.columns:
            # Zastąp ujemne wartości 0
            df_cleaned[col] = df_cleaned[col].clip(lower=0)
            # Zastąp NaN 0
            df_cleaned[col] = df_cleaned[col].fillna(0)
    
    # 8. Wyczyść kolumny boolean
    boolean_columns = ['user_verified', 'is_retweet']
    for col in boolean_columns:
        if col in df_cleaned.columns:
            # Zastąp NaN False
            df_cleaned[col] = df_cleaned[col].fillna(False)
    
    # 9. Wyczyść source
    if 'source' in df_cleaned.columns:
        df_cleaned['source'] = df_cleaned['source'].fillna('Unknown')
    
    final_count = len(df_cleaned)
    removed_count = original_count - final_count
    
    print(f"  Usunięto rekordów: {removed_count:,}")
    print(f"  Pozostało rekordów: {final_count:,}")
    print(f"  Procent zachowanych: {(final_count/original_count)*100:.1f}%")
    
    return df_cleaned


def validate_data_quality(df):
    """Sprawdź jakość danych po czyszczeniu."""
    print(f"\n✅ Walidacja jakości danych:")
    
    import pandas as pd
    
    issues = []
    
    # Sprawdź czy user_name jest unikalny
    if df['user_name'].duplicated().any():
        issues.append("Duplikaty w user_name")
    else:
        print("  ✅ user_name jest unikalny")
    
    # Sprawdź czy nie ma pustych tekstów
    text_cols = ['user_name', 'text']
    for col in text_cols:
        if (df[col].str.strip() == '').any():
            issues.append(f"Puste wartości w {col}")
        else:
            print(f"  ✅ {col} nie zawiera pustych wartości")
    
    # Sprawdź czy daty są prawidłowe
    try:
        pd.to_datetime(df['date'], errors='raise')
        print("  ✅ Daty są prawidłowe")
    except:
        issues.append("Nieprawidłowe daty")
    
    # Sprawdź czy kolumny numeryczne są nieujemne
    numeric_cols = ['user_followers', 'user_friends', 'user_favourites']
    for col in numeric_cols:
        if col in df.columns:
            if (df[col] < 0).any():
                issues.append(f"Ujemne wartości w {col}")
            else:
                print(f"  ✅ {col} zawiera tylko nieujemne wartości")
    
    if issues:
        print(f"\n⚠️  Znalezione problemy:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"\n🎉 Wszystkie dane przeszły walidację!")
    
    return len(issues) == 0


def clear_databases(pg: PostgresManager, mg: MongoManager):
    """Wyczyść bazy danych przed ładowaniem nowych danych."""
    print("\n🧹 Czyszczenie baz danych...")
    
    # PostgreSQL - usuń wszystkie tabele
    with pg.conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tweet_hashtags CASCADE")
        cur.execute("DROP TABLE IF EXISTS tweets CASCADE")
        cur.execute("DROP TABLE IF EXISTS hashtags CASCADE")
        cur.execute("DROP TABLE IF EXISTS sources CASCADE")
        cur.execute("DROP TABLE IF EXISTS users CASCADE")
    pg.conn.commit()
    
    # MongoDB - usuń kolekcję
    mg.col.drop()
    
    print("✅ Bazy danych wyczyszczone!")


def run_basic_benchmarks(pg: PostgresManager, mg: MongoManager):
    """Uruchom podstawowe testy wydajności."""
    print("\n🔍 Uruchamianie podstawowych benchmarków...")
    
    # Test 1: Liczenie rekordów
    print("\n📊 Test 1: Liczenie rekordów")
    
    # PostgreSQL
    start = time.perf_counter()
    with pg.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM tweets")
        pg_tweet_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users")
        pg_user_count = cur.fetchone()[0]
    pg_time = time.perf_counter() - start
    
    # MongoDB
    start = time.perf_counter()
    mongo_tweet_count = mg.col.count_documents({})
    mongo_user_count = len(mg.col.distinct("user.user_name"))
    mongo_time = time.perf_counter() - start
    
    print(f"  PostgreSQL: {pg_tweet_count} tweetów, {pg_user_count} użytkowników w {pg_time:.4f}s")
    print(f"  MongoDB:    {mongo_tweet_count} tweetów, {mongo_user_count} użytkowników w {mongo_time:.4f}s")
    
    # Test 2: Pobieranie najnowszych tweetów
    print("\n📊 Test 2: Najnowsze tweety (100 rekordów)")
    
    # PostgreSQL
    start = time.perf_counter()
    with pg.conn.cursor() as cur:
        cur.execute("""
            SELECT t.text, u.user_name, t.date 
            FROM tweets t 
            JOIN users u ON t.user_id = u.id 
            ORDER BY t.date DESC 
            LIMIT 100
        """)
        pg_recent = cur.fetchall()
    pg_time = time.perf_counter() - start
    
    # MongoDB
    start = time.perf_counter()
    mongo_recent = list(mg.col.find(
        {},
        {"text": 1, "user.user_name": 1, "date": 1}
    ).sort("date", -1).limit(100))
    mongo_time = time.perf_counter() - start
    
    print(f"  PostgreSQL: {len(pg_recent)} rekordów w {pg_time:.4f}s")
    print(f"  MongoDB:    {len(mongo_recent)} rekordów w {mongo_time:.4f}s")
    
    # Test 3: Wyszukiwanie po hashtagach
    print("\n📊 Test 3: Wyszukiwanie po hashtagach")
    
    # PostgreSQL
    start = time.perf_counter()
    with pg.conn.cursor() as cur:
        cur.execute("""
            SELECT t.text, u.user_name, h.tag
            FROM tweets t
            JOIN users u ON t.user_id = u.id
            JOIN tweet_hashtags th ON t.id = th.tweet_id
            JOIN hashtags h ON th.hashtag_id = h.id
            WHERE h.tag = 'bitcoin'
            ORDER BY t.date DESC
            LIMIT 50
        """)
        pg_hashtag = cur.fetchall()
    pg_time = time.perf_counter() - start
    
    # MongoDB
    start = time.perf_counter()
    mongo_hashtag = list(mg.col.find(
        {"hashtags": "bitcoin"},
        {"text": 1, "user.user_name": 1, "hashtags": 1}
    ).sort("date", -1).limit(50))
    mongo_time = time.perf_counter() - start
    
    print(f"  PostgreSQL: {len(pg_hashtag)} rekordów w {pg_time:.4f}s")
    print(f"  MongoDB:    {len(mongo_hashtag)} rekordów w {mongo_time:.4f}s")
    
    print("\n✅ Podstawowe benchmarki zakończone!")


def run():
    print("🔌 Łączenie z Postgres & Mongo...")
    pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
    mg = MongoManager(MONGO_URI, MONGO_DB)

    # Wyczyść bazy danych
    clear_databases(pg, mg)

    print("🧱 Inicjalizacja schematu / indeksów...")
    pg.init_schema()
    mg.init_indexes()  # indeksy: date, user.user_name, hashtags, is_retweet

    # Analiza i czyszczenie danych CSV
    print(f"\n📊 Analiza i czyszczenie danych CSV...")
    df_raw = analyze_csv_data(CSV_PATH)
    df_cleaned = clean_csv_data(df_raw)
    data_quality_ok = validate_data_quality(df_cleaned)
    
    if not data_quality_ok:
        print("⚠️  Dane zawierają problemy jakościowe. Kontynuujesz? (Enter aby kontynuować)")
        input()
    
    print(f"\n📥 Wczytywanie wyczyszczonych danych...")
    # Konwertuj DataFrame z powrotem na generator
    rows_iter = (row.to_dict() for _, row in df_cleaned.iterrows())

    mongo_ops: List[InsertOne] = []
    total = 0
    t0 = time.perf_counter()

    for row in rows_iter:
        # ---------- PostgreSQL (model relacyjny) ----------
        user_id = pg.upsert_user(row)
        source_id = pg.get_or_create_source(row.get("source"))
        tweet_id = pg.insert_tweet(user_id, row, source_id)

        for tag in (row.get("hashtags") or []):
            tag = (str(tag) or "").strip().lstrip("#").lower()
            if not tag:
                continue
            hid = pg.get_or_create_hashtag(tag)
            pg.link_tweet_hashtag(tweet_id, hid)

        # ---------- MongoDB (1 kolekcja: 'tweets') ----------
        mongo_ops.append(InsertOne(build_mongo_doc(row)))

        total += 1
        if total % BATCH_SIZE == 0:
            # flush Postgres
            flush_postgres(pg)
            # flush Mongo
            inserted = flush_mongo(mg, mongo_ops)
            mongo_ops.clear()
            print(f"✅ Batch {total // BATCH_SIZE}: zapisano {BATCH_SIZE} rekordów (Mongo inserted={inserted}).")

    # final flush
    flush_postgres(pg)
    if mongo_ops:
        inserted = flush_mongo(mg, mongo_ops)
        mongo_ops.clear()
        print(f"✅ Finalny batch: Mongo inserted={inserted}")

    elapsed = time.perf_counter() - t0
    print(f"🎉 Gotowe! Załadowano łącznie {total} rekordów w {elapsed:.2f}s.")

    # Uruchom podstawowe benchmarki
    run_basic_benchmarks(pg, mg)

    # zamykanie
    pg.close()
    mg.close()

if __name__ == "__main__":
    run()
