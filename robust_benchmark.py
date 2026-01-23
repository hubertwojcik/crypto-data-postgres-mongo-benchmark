#!/usr/bin/env python3
"""
Odporny benchmark z obsługą błędów i timeout - wszystkie 20 operacji CRUD dla 10K, 100K, 1M.
"""

import sys
import os
import time
import pandas as pd
import numpy as np
import json
from datetime import datetime
import signal
import threading
from contextlib import contextmanager

# Dodaj src do ścieżki
sys.path.append('src')

# Importy z projektu
from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager
from src.db.data_precleaner import DataPrecleaner

class TimeoutError(Exception):
    pass

@contextmanager
def timeout(seconds):
    """Context manager dla timeout."""
    def signal_handler(signum, frame):
        raise TimeoutError(f"Operacja przekroczyła limit czasu {seconds}s")
    
    # Ustaw handler
    old_handler = signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

def safe_execute(func, test_name, timeout_seconds=300):
    """Bezpieczne wykonanie funkcji z timeout i obsługą błędów."""
    print(f"    🔄 {test_name}...", end=" ", flush=True)
    
    try:
        with timeout(timeout_seconds):
            start_time = time.perf_counter()
            result = func()
            elapsed = time.perf_counter() - start_time
            
            if isinstance(result, tuple) and len(result) == 2:
                # Para wyników (pg, mongo)
                pg_result, mongo_result = result
                pg_time = pg_result.get("time", elapsed) if isinstance(pg_result, dict) else elapsed
                mongo_time = mongo_result.get("time", elapsed) if isinstance(mongo_result, dict) else elapsed
                
                print(f"✅ PG={pg_time:.6f}s, Mongo={mongo_time:.6f}s")
                return {
                    "postgresql": pg_time,
                    "mongodb": mongo_time,
                    "status": "success"
                }
            else:
                # Pojedynczy wynik
                test_time = result.get("time", elapsed) if isinstance(result, dict) else elapsed
                print(f"✅ {test_time:.6f}s")
                return {
                    "time": test_time,
                    "status": "success"
                }
                
    except TimeoutError as e:
        print(f"⏰ TIMEOUT ({timeout_seconds}s)")
        return {
            "postgresql": 0.0,
            "mongodb": 0.0,
            "status": "timeout",
            "error": str(e)
        }
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ ERROR: {error_msg}")
        return {
            "postgresql": 0.0,
            "mongodb": 0.0,
            "status": "error",
            "error": error_msg
        }

def run_robust_benchmark():
    """Uruchom odporny benchmark z pełną obsługą błędów."""
    print("🚀 Odporny benchmark PostgreSQL vs MongoDB - Wszystkie 20 operacji CRUD")
    print("="*90)
    
    # Inicjalizacja
    print("\n📦 Inicjalizacja...")
    try:
        cleaner = DataPrecleaner()
        pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
        mg = MongoManager(MONGO_URI, MONGO_DB)
        print("✅ Połączenia nawiązane!")
    except Exception as e:
        print(f"❌ Błąd inicjalizacji: {e}")
        return None
    
    results = {}
    
    # Zbiory danych do testowania
    datasets = [
        ("10K", "data/Bitcoin_tweets_10k.csv", 10000, 60),      # 60s timeout
        ("100K", "data/Bitcoin_tweets_100k.csv", 100000, 300),  # 5min timeout
        ("1M", "data/Bitcoin_tweets_1M.csv", 1000000, 1800)     # 30min timeout
    ]
    
    for dataset_name, csv_path, expected_size, dataset_timeout in datasets:
        print(f"\n{'='*90}")
        print(f"DATASET: {dataset_name} (timeout: {dataset_timeout//60}min)")
        print(f"{'='*90}")
        
        dataset_start = time.perf_counter()
        
        try:
            # 1. Wczytanie danych
            print(f"\n📁 Wczytywanie danych z {csv_path}")
            if not os.path.exists(csv_path):
                print(f"❌ Plik {csv_path} nie istnieje!")
                continue
            
            try:
                with timeout(120):  # 2min na wczytanie
                    df_raw = pd.read_csv(csv_path)
                    print(f"📊 Załadowano {len(df_raw):,} rekordów")
            except TimeoutError:
                print(f"⏰ Timeout podczas wczytywania {csv_path}")
                continue
            except Exception as e:
                print(f"❌ Błąd wczytywania: {e}")
                continue
            
            # 2. Czyszczenie danych
            print("🧹 Czyszczenie danych...")
            try:
                with timeout(300):  # 5min na czyszczenie
                    df_clean, clean_time = cleaner.clean_data_timed(df_raw)
                    print(f"✅ Dane wyczyszczone w {clean_time:.4f}s ({len(df_clean):,} rekordów)")
            except TimeoutError:
                print(f"⏰ Timeout podczas czyszczenia danych")
                continue
            except Exception as e:
                print(f"❌ Błąd czyszczenia: {e}")
                continue
            
            # 3. Przygotowanie baz danych
            print("🗑️  Czyszczenie baz danych...")
            try:
                with timeout(60):
                    pg.clear_database()
                    mg.clear_database()
                    print("✅ Bazy danych wyczyszczone")
            except Exception as e:
                print(f"❌ Błąd czyszczenia baz: {e}")
                continue
            
            print("🧱 Inicjalizacja schematów...")
            try:
                with timeout(60):
                    pg.init_schema()
                    mg.init_indexes()
                    print("✅ Schematy zainicjalizowane")
            except Exception as e:
                print(f"❌ Błąd inicjalizacji schematów: {e}")
                continue
            
            print("🧹 Czyszczenie cache baz danych...")
            try:
                with timeout(60):
                    pg.clear_cache()
                    mg.clear_cache()
                    print("✅ Cache wyczyszczony")
            except Exception as e:
                print(f"⚠️  Błąd czyszczenia cache: {e}")
                # Kontynuuj mimo błędu cache
            
            # 4. Ładowanie danych
            print(f"\n📥 Ładowanie {len(df_clean):,} rekordów...")
            
            # PostgreSQL
            pg_load_result = safe_execute(
                lambda: pg.load_data_from_dataframe(df_clean, batch_size=1000),
                "PostgreSQL loading",
                timeout_seconds=dataset_timeout//2
            )
            pg_load_time = pg_load_result.get("time", 0.0) if "time" in pg_load_result else pg_load_result.get("postgresql", 0.0)
            
            # MongoDB
            mongo_load_result = safe_execute(
                lambda: mg.load_data_from_dataframe(df_clean, batch_size=1000),
                "MongoDB loading", 
                timeout_seconds=dataset_timeout//2
            )
            mongo_load_time = mongo_load_result.get("time", 0.0) if "time" in mongo_load_result else mongo_load_result.get("mongodb", 0.0)
            
            # 5. Testy CRUD - wszystkie 20 operacji
            print(f"\n🧪 Testy CRUD - wszystkie 20 operacji")
            
            # Przygotuj dane testowe
            try:
                sample_row = df_clean.iloc[0].to_dict()
            except Exception as e:
                print(f"❌ Błąd przygotowania danych testowych: {e}")
                continue
                
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
                result = safe_execute(test_func, test_name, timeout_seconds=120)
                crud_results[f"READ: {test_name}"] = result
            
            # CREATE testy - wszystkie 4 operacje
            print("  ✏️  CREATE testy (4 operacje):")
            
            create_tests = [
                ("New Tweet with New User", lambda: (pg.test_create(sample_row), mg.test_create(sample_row))),
                ("Tweet with Existing User", lambda: (pg.test_create_with_existing_user(sample_row), mg.test_create_with_existing_user(sample_row))),
                ("Tweet with Many Hashtags", lambda: (pg.test_create_with_many_hashtags(sample_row, num_hashtags=10), mg.test_create_with_many_hashtags(sample_row, num_hashtags=10))),
                ("Batch Insert", lambda: (pg.test_batch_insert(df_clean.head(100).to_dict('records'), batch_size=50), mg.test_batch_insert(df_clean.head(100).to_dict('records'), batch_size=50)))
            ]
            
            for test_name, test_func in create_tests:
                result = safe_execute(test_func, test_name, timeout_seconds=120)
                crud_results[f"CREATE: {test_name}"] = result
            
            # UPDATE testy - wszystkie 3 operacje
            print("  🔄 UPDATE testy (3 operacje):")
            
            update_tests = [
                ("Tweet Text", lambda: (pg.test_update(), mg.test_update())),
                ("User Data", lambda: (pg.test_update_user_data(), mg.test_update_user_data())),
                ("Bulk Update", lambda: (pg.test_bulk_update(days_ago=365), mg.test_bulk_update(days_ago=365)))
            ]
            
            for test_name, test_func in update_tests:
                result = safe_execute(test_func, test_name, timeout_seconds=300)  # Bulk update może trwać dłużej
                crud_results[f"UPDATE: {test_name}"] = result
            
            # DELETE testy - wszystkie 3 operacje
            print("  🗑️  DELETE testy (3 operacje):")
            
            delete_tests = [
                ("Single Tweet", lambda: (pg.test_delete(), mg.test_delete())),
                ("User's All Tweets", lambda: (pg.test_delete_user_tweets(), mg.test_delete_user_tweets())),
                ("Old Tweets", lambda: (pg.test_delete_old_tweets(days_ago=365), mg.test_delete_old_tweets(days_ago=365)))
            ]
            
            for test_name, test_func in delete_tests:
                result = safe_execute(test_func, test_name, timeout_seconds=300)  # DELETE może trwać dłużej
                crud_results[f"DELETE: {test_name}"] = result
            
            # Zapisz wyniki
            dataset_total = time.perf_counter() - dataset_start
            
            results[dataset_name] = {
                "dataset_info": {
                    "name": dataset_name,
                    "records": len(df_clean),
                    "clean_time": clean_time,
                    "total_time": dataset_total
                },
                "load_times": {
                    "postgresql": pg_load_time,
                    "mongodb": mongo_load_time
                },
                "crud_results": crud_results
            }
            
            # Podsumowanie dla tego zbioru
            successful_tests = sum(1 for r in crud_results.values() if r.get("status") == "success")
            failed_tests = sum(1 for r in crud_results.values() if r.get("status") in ["error", "timeout"])
            
            print(f"\n✅ Dataset {dataset_name} zakończony w {dataset_total:.2f}s!")
            print(f"📊 Testy: {successful_tests} udane, {failed_tests} nieudane")
            
        except Exception as e:
            print(f"\n❌ Krytyczny błąd w dataset {dataset_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Zapisz wyniki
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"robust_benchmark_results_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Wyniki zapisane do: {filename}")
    except Exception as e:
        print(f"❌ Błąd zapisu wyników: {e}")
    
    # Podsumowanie końcowe
    print(f"\n📊 PODSUMOWANIE KOŃCOWE")
    print("="*100)
    
    for dataset_name, data in results.items():
        print(f"\n🔍 {dataset_name} ({data['dataset_info']['records']:,} rekordów)")
        print(f"⏱️  Całkowity czas: {data['dataset_info']['total_time']:.2f}s")
        
        # Statystyki testów
        crud_results = data["crud_results"]
        successful = sum(1 for r in crud_results.values() if r.get("status") == "success")
        timeouts = sum(1 for r in crud_results.values() if r.get("status") == "timeout")
        errors = sum(1 for r in crud_results.values() if r.get("status") == "error")
        
        print(f"📊 Testy CRUD: {successful}/20 udane, {timeouts} timeout, {errors} błędów")
        
        # Czasy ładowania
        pg_load = data["load_times"]["postgresql"]
        mongo_load = data["load_times"]["mongodb"]
        if pg_load > 0 and mongo_load > 0:
            load_ratio = pg_load / mongo_load
            print(f"📥 Ładowanie: PG={pg_load:.2f}s, Mongo={mongo_load:.2f}s (ratio: {load_ratio:.1f}x)")
    
    # Zamknij połączenia
    try:
        pg.close()
        mg.close()
        print("\n✅ Połączenia zamknięte")
    except:
        pass
    
    print(f"\n🎉 Odporny benchmark zakończony!")
    print(f"📊 Przetestowano {len(results)} zbiorów danych")
    return results

if __name__ == "__main__":
    try:
        results = run_robust_benchmark()
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark przerwany przez użytkownika")
    except Exception as e:
        print(f"\n❌ Krytyczny błąd: {e}")
        import traceback
        traceback.print_exc()
