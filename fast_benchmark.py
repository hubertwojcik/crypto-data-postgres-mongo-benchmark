#!/usr/bin/env python3
"""
Szybki benchmark bez czyszczenia cache systemu - wszystkie 20 operacji CRUD.
"""

import sys
import os
import time
import pandas as pd
import numpy as np
import json
from datetime import datetime

# Dodaj src do ścieżki
sys.path.append('src')

# Importy z projektu
from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager
from src.db.data_precleaner import DataPrecleaner

def run_fast_benchmark():
    """Uruchom szybki benchmark dla wszystkich operacji."""
    print("🚀 Szybki benchmark PostgreSQL vs MongoDB - Wszystkie 20 operacji CRUD")
    print("="*80)
    
    # Inicjalizacja
    print("\n📦 Inicjalizacja...")
    cleaner = DataPrecleaner()
    pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
    mg = MongoManager(MONGO_URI, MONGO_DB)
    print("✅ Połączenia nawiązane!")
    
    results = {}
    
    # Zbiory danych do testowania
    datasets = [
        ("10K", "data/Bitcoin_tweets_10k.csv", 10000),
        ("100K", "data/Bitcoin_tweets_100k.csv", 100000),
        ("1M", "data/Bitcoin_tweets_1M.csv", 1000000)
    ]
    
    for dataset_name, csv_path, expected_size in datasets:
        print(f"\n{'='*80}")
        print(f"DATASET: {dataset_name}")
        print(f"{'='*80}")
        
        try:
            # 1. Wczytanie danych
            print(f"\n📁 Wczytywanie danych z {csv_path}")
            if not os.path.exists(csv_path):
                print(f"❌ Plik {csv_path} nie istnieje!")
                continue
                
            df_raw = pd.read_csv(csv_path)
            print(f"📊 Załadowano {len(df_raw):,} rekordów")
            
            # 2. Czyszczenie danych
            print("🧹 Czyszczenie danych...")
            df_clean, clean_time = cleaner.clean_data_timed(df_raw)
            print(f"✅ Dane wyczyszczone w {clean_time:.4f}s ({len(df_clean):,} rekordów)")
            
            # 3. Przygotowanie baz danych
            print("🗑️  Czyszczenie baz danych...")
            pg.clear_database()
            mg.clear_database()
            
            print("🧱 Inicjalizacja schematów...")
            pg.init_schema()
            mg.init_indexes()
            
            print("🧹 Czyszczenie cache baz danych...")
            # Tylko cache baz danych, bez systemu
            pg.clear_cache()
            mg.clear_cache()
            
            # 4. Ładowanie danych
            print(f"\n📥 Ładowanie {len(df_clean):,} rekordów...")
            
            print("  PostgreSQL...", end=" ", flush=True)
            pg_load_time = pg.load_data_from_dataframe(df_clean, batch_size=1000)
            print(f"✅ {pg_load_time:.4f}s")
            
            print("  MongoDB...", end=" ", flush=True)
            mongo_load_time = mg.load_data_from_dataframe(df_clean, batch_size=1000)
            print(f"✅ {mongo_load_time:.4f}s")
            
            # 5. Testy CRUD - wszystkie 20 operacji
            print(f"\n🧪 Testy CRUD - wszystkie 20 operacji")
            
            # Przygotuj dane testowe
            sample_row = df_clean.iloc[0].to_dict()
            crud_results = {}
            
            # READ testy - wszystkie 10 operacji
            print("  📖 READ testy (10 operacji):")
            
            read_tests = [
                ("Count All Tweets", lambda: (pg.test_read_count(), mg.test_read_count())),
                ("Recent Tweets", lambda: (pg.test_read_recent(limit=100), mg.test_read_recent(limit=100))),
                ("Search by Hashtag", lambda: (pg.test_read_hashtag(hashtag="bitcoin", limit=50), mg.test_read_hashtag(hashtag="bitcoin", limit=50))),
                ("User Statistics", lambda: (pg.test_read_user_stats(limit=20), mg.test_read_user_stats(limit=20))),
                ("Popular Hashtags", lambda: (pg.test_read_popular_hashtags(limit=20), mg.test_read_popular_hashtags(limit=20))),
                ("Daily Statistics", lambda: (pg.test_read_daily_stats(days=30), mg.test_read_daily_stats(days=30))),
                ("Complex Aggregations", lambda: (pg.test_read_aggregate_joins(), mg.test_read_aggregate_joins())),
                ("User Ranking", lambda: (pg.test_read_user_ranking(), mg.test_read_user_ranking())),
                ("Daily Activity", lambda: (pg.test_read_daily_activity(), mg.test_read_daily_activity())),
                ("Hashtag Trends", lambda: (pg.test_read_hashtag_trends(), mg.test_read_hashtag_trends()))
            ]
            
            for test_name, test_func in read_tests:
                try:
                    pg_result, mongo_result = test_func()
                    crud_results[f"READ: {test_name}"] = {
                        "postgresql": pg_result["time"],
                        "mongodb": mongo_result["time"]
                    }
                    print(f"    ✅ {test_name}: PG={pg_result['time']:.6f}s, Mongo={mongo_result['time']:.6f}s")
                except Exception as e:
                    print(f"    ❌ {test_name}: {str(e)[:50]}...")
                    crud_results[f"READ: {test_name}"] = {"postgresql": 0.0, "mongodb": 0.0}
            
            # CREATE testy - wszystkie 4 operacje
            print("  ✏️  CREATE testy (4 operacje):")
            
            create_tests = [
                ("New Tweet with New User", lambda: (pg.test_create(sample_row), mg.test_create(sample_row))),
                ("Tweet with Existing User", lambda: (pg.test_create_with_existing_user(sample_row), mg.test_create_with_existing_user(sample_row))),
                ("Tweet with Many Hashtags", lambda: (pg.test_create_with_many_hashtags(sample_row, num_hashtags=10), mg.test_create_with_many_hashtags(sample_row, num_hashtags=10))),
                ("Batch Insert", lambda: (pg.test_batch_insert(df_clean.head(100).to_dict('records'), batch_size=50), mg.test_batch_insert(df_clean.head(100).to_dict('records'), batch_size=50)))
            ]
            
            for test_name, test_func in create_tests:
                try:
                    pg_result, mongo_result = test_func()
                    crud_results[f"CREATE: {test_name}"] = {
                        "postgresql": pg_result["time"],
                        "mongodb": mongo_result["time"]
                    }
                    print(f"    ✅ {test_name}: PG={pg_result['time']:.6f}s, Mongo={mongo_result['time']:.6f}s")
                except Exception as e:
                    print(f"    ❌ {test_name}: {str(e)[:50]}...")
                    crud_results[f"CREATE: {test_name}"] = {"postgresql": 0.0, "mongodb": 0.0}
            
            # UPDATE testy - wszystkie 3 operacje
            print("  🔄 UPDATE testy (3 operacje):")
            
            update_tests = [
                ("Tweet Text", lambda: (pg.test_update(), mg.test_update())),
                ("User Data", lambda: (pg.test_update_user_data(), mg.test_update_user_data())),
                ("Bulk Update", lambda: (pg.test_bulk_update(days_ago=365), mg.test_bulk_update(days_ago=365)))
            ]
            
            for test_name, test_func in update_tests:
                try:
                    pg_result, mongo_result = test_func()
                    crud_results[f"UPDATE: {test_name}"] = {
                        "postgresql": pg_result["time"],
                        "mongodb": mongo_result["time"]
                    }
                    print(f"    ✅ {test_name}: PG={pg_result['time']:.6f}s, Mongo={mongo_result['time']:.6f}s")
                except Exception as e:
                    print(f"    ❌ {test_name}: {str(e)[:50]}...")
                    crud_results[f"UPDATE: {test_name}"] = {"postgresql": 0.0, "mongodb": 0.0}
            
            # DELETE testy - wszystkie 3 operacje
            print("  🗑️  DELETE testy (3 operacje):")
            
            delete_tests = [
                ("Single Tweet", lambda: (pg.test_delete(), mg.test_delete())),
                ("User's All Tweets", lambda: (pg.test_delete_user_tweets(), mg.test_delete_user_tweets())),
                ("Old Tweets", lambda: (pg.test_delete_old_tweets(days_ago=365), mg.test_delete_old_tweets(days_ago=365)))
            ]
            
            for test_name, test_func in delete_tests:
                try:
                    pg_result, mongo_result = test_func()
                    crud_results[f"DELETE: {test_name}"] = {
                        "postgresql": pg_result["time"],
                        "mongodb": mongo_result["time"]
                    }
                    print(f"    ✅ {test_name}: PG={pg_result['time']:.6f}s, Mongo={mongo_result['time']:.6f}s")
                except Exception as e:
                    print(f"    ❌ {test_name}: {str(e)[:50]}...")
                    crud_results[f"DELETE: {test_name}"] = {"postgresql": 0.0, "mongodb": 0.0}
            
            # Zapisz wyniki
            results[dataset_name] = {
                "dataset_info": {
                    "name": dataset_name,
                    "records": len(df_clean),
                    "clean_time": clean_time
                },
                "load_times": {
                    "postgresql": pg_load_time,
                    "mongodb": mongo_load_time
                },
                "crud_results": crud_results
            }
            
            print(f"\n✅ Dataset {dataset_name} zakończony!")
            print(f"📊 Wykonano {len(crud_results)} testów CRUD")
            
        except Exception as e:
            print(f"\n❌ Błąd w dataset {dataset_name}: {e}")
            continue
    
    # Zapisz wyniki
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"full_benchmark_results_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Wyniki zapisane do: {filename}")
    
    # Podsumowanie
    print(f"\n📊 PODSUMOWANIE WYNIKÓW")
    print("="*100)
    
    for dataset_name, data in results.items():
        print(f"\n🔍 {dataset_name} ({data['dataset_info']['records']:,} rekordów)")
        print("-" * 60)
        
        # Czasy ładowania
        pg_load = data["load_times"]["postgresql"]
        mongo_load = data["load_times"]["mongodb"]
        load_ratio = pg_load / mongo_load if mongo_load > 0 else 0
        
        print(f"📥 Ładowanie danych:")
        print(f"  PostgreSQL: {pg_load:.4f}s")
        print(f"  MongoDB:    {mongo_load:.4f}s")
        print(f"  Stosunek:   {load_ratio:.2f}x ({'MongoDB szybszy' if load_ratio > 1 else 'PostgreSQL szybszy'})")
        
        # Średnie czasy CRUD według typu operacji
        crud_results = data["crud_results"]
        
        for op_type in ["READ", "CREATE", "UPDATE", "DELETE"]:
            op_results = {k: v for k, v in crud_results.items() if k.startswith(op_type)}
            if op_results:
                pg_times = [r["postgresql"] for r in op_results.values() if r["postgresql"] > 0]
                mongo_times = [r["mongodb"] for r in op_results.values() if r["mongodb"] > 0]
                
                if pg_times and mongo_times:
                    pg_avg = np.mean(pg_times)
                    mongo_avg = np.mean(mongo_times)
                    crud_ratio = pg_avg / mongo_avg if mongo_avg > 0 else 0
                    
                    print(f"🧪 Średnie {op_type}:")
                    print(f"  PostgreSQL: {pg_avg:.6f}s")
                    print(f"  MongoDB:    {mongo_avg:.6f}s")
                    print(f"  Stosunek:   {crud_ratio:.2f}x ({'MongoDB szybszy' if crud_ratio > 1 else 'PostgreSQL szybszy'})")
    
    # Zamknij połączenia
    try:
        pg.close()
        mg.close()
        print("\n✅ Połączenia zamknięte")
    except:
        pass
    
    print(f"\n🎉 Pełny benchmark zakończony!")
    print(f"📊 Przetestowano {len(results)} zbiorów danych")
    print(f"🧪 Wykonano wszystkie 20 operacji CRUD dla każdego zbioru")
    return results

if __name__ == "__main__":
    try:
        results = run_fast_benchmark()
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark przerwany przez użytkownika")
    except Exception as e:
        print(f"\n❌ Błąd: {e}")
        import traceback
        traceback.print_exc()
