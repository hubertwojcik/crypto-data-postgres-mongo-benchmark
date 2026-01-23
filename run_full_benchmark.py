#!/usr/bin/env python3
"""
Automatyczny skrypt do przeprowadzenia pełnych testów wydajności
PostgreSQL vs MongoDB dla różnych rozmiarów danych.
"""

import sys
import os
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any
import json
from datetime import datetime

# Dodaj src do ścieżki
sys.path.append('src')

# Importy z projektu
from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager
from src.db.data_precleaner import DataPrecleaner

class BenchmarkRunner:
    """Klasa do automatycznego przeprowadzania testów wydajności."""
    
    def __init__(self):
        """Inicjalizacja połączeń i narzędzi."""
        print("🚀 Inicjalizacja BenchmarkRunner...")
        
        # Inicjalizacja narzędzi
        self.cleaner = DataPrecleaner()
        
        # Połączenia z bazami danych
        self.pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
        self.mg = MongoManager(MONGO_URI, MONGO_DB)
        
        # Konfiguracja wykresów
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (15, 10)
        
        # Wyniki eksperymentów
        self.results = {}
        
        print("✅ BenchmarkRunner zainicjalizowany!")
    
    def run_dataset_experiment(self, dataset_name: str, csv_path: str, sample_size: int = None):
        """
        Przeprowadź pełny eksperyment dla danego zbioru danych.
        
        Args:
            dataset_name: Nazwa zbioru danych (np. "10K", "100K", "1M")
            csv_path: Ścieżka do pliku CSV
            sample_size: Opcjonalny limit rekordów do załadowania
        """
        print(f"\n{'='*80}")
        print(f"EKSPERYMENT: {dataset_name} rekordów")
        print(f"{'='*80}")
        
        experiment_start = time.perf_counter()
        
        # 1. WCZYTANIE I PRZYGOTOWANIE DANYCH
        print(f"\n📁 Krok 1: Wczytywanie danych z {csv_path}")
        df_raw = pd.read_csv(csv_path)
        
        if sample_size:
            df_raw = df_raw.head(sample_size)
            print(f"📊 Ograniczono do {sample_size:,} rekordów")
        
        print(f"📊 Załadowano {len(df_raw):,} rekordów")
        
        # Analiza danych
        analysis = self.cleaner.analyze_data(df_raw)
        
        # Czyszczenie danych
        df_clean, clean_time = self.cleaner.clean_data_timed(df_raw)
        
        print(f"📈 Podsumowanie przygotowania danych:")
        print(f"  Przed czyszczeniem: {analysis['total_records']:,} rekordów")
        print(f"  Po czyszczeniu: {len(df_clean):,} rekordów")
        print(f"  Czas czyszczenia: {clean_time:.4f}s")
        
        # 2. CZYSZCZENIE BAZ DANYCH I CACHE
        print(f"\n🧹 Krok 2: Czyszczenie baz danych i cache")
        self.clear_databases_and_cache()
        
        # 3. INICJALIZACJA SCHEMATÓW
        print(f"\n🧱 Krok 3: Inicjalizacja schematów")
        self.pg.init_schema()
        self.mg.init_indexes()
        print("✅ Schematy zainicjalizowane!")
        
        # 4. POPULACJA BAZ DANYCH
        print(f"\n📥 Krok 4: Populacja baz danych")
        
        # PostgreSQL
        print("  📊 Ładowanie do PostgreSQL...")
        pg_load_start = time.perf_counter()
        pg_load_time = self.pg.load_data_from_dataframe(df_clean, batch_size=1000)
        
        # MongoDB
        print("  📊 Ładowanie do MongoDB...")
        mongo_load_start = time.perf_counter()
        mongo_load_time = self.mg.load_data_from_dataframe(df_clean, batch_size=1000)
        
        print(f"✅ Populacja zakończona!")
        print(f"  PostgreSQL: {pg_load_time:.4f}s")
        print(f"  MongoDB: {mongo_load_time:.4f}s")
        
        # 5. TESTY CRUD
        print(f"\n🧪 Krok 5: Testy CRUD")
        crud_results = self.run_crud_tests(df_clean)
        
        # 6. ZAPISANIE WYNIKÓW
        experiment_total = time.perf_counter() - experiment_start
        
        self.results[dataset_name] = {
            "dataset_info": {
                "name": dataset_name,
                "csv_path": csv_path,
                "records_raw": len(df_raw),
                "records_clean": len(df_clean),
                "clean_time": clean_time
            },
            "load_times": {
                "postgresql": pg_load_time,
                "mongodb": mongo_load_time
            },
            "crud_results": crud_results,
            "total_experiment_time": experiment_total
        }
        
        print(f"\n✅ Eksperyment {dataset_name} zakończony w {experiment_total:.2f}s")
        return self.results[dataset_name]
    
    def clear_databases_and_cache(self):
        """Wyczyść bazy danych i cache."""
        print("  🧹 Czyszczenie baz danych...")
        self.pg.clear_database()
        self.mg.clear_database()
        
        print("  🧹 Czyszczenie cache...")
        self.pg.clear_all_caches()
        self.mg.clear_all_caches()
        
        print("  ✅ Bazy danych i cache wyczyszczone!")
    
    def run_crud_tests(self, df_sample):
        """Przeprowadź wszystkie testy CRUD."""
        print("  🧹 Czyszczenie cache przed testami CRUD...")
        self.pg.clear_all_caches()
        self.mg.clear_all_caches()
        
        crud_results = {}
        sample_row = df_sample.iloc[0].to_dict()
        
        # ========== READ TESTS ==========
        print("  📖 Testy READ...")
        
        read_tests = [
            ("Count All Tweets", lambda: (self.pg.test_read_count(), self.mg.test_read_count())),
            ("Recent Tweets", lambda: (self.pg.test_read_recent(limit=100), self.mg.test_read_recent(limit=100))),
            ("Search by Hashtag", lambda: (self.pg.test_read_hashtag(hashtag="bitcoin", limit=50), self.mg.test_read_hashtag(hashtag="bitcoin", limit=50))),
            ("User Statistics", lambda: (self.pg.test_read_user_stats(limit=20), self.mg.test_read_user_stats(limit=20))),
            ("Popular Hashtags", lambda: (self.pg.test_read_popular_hashtags(limit=20), self.mg.test_read_popular_hashtags(limit=20))),
            ("Daily Statistics", lambda: (self.pg.test_read_daily_stats(days=30), self.mg.test_read_daily_stats(days=30))),
            ("Complex Aggregations", lambda: (self.pg.test_read_aggregate_joins(), self.mg.test_read_aggregate_joins())),
            ("User Ranking", lambda: (self.pg.test_read_user_ranking(), self.mg.test_read_user_ranking())),
            ("Daily Activity", lambda: (self.pg.test_read_daily_activity(), self.mg.test_read_daily_activity())),
            ("Hashtag Trends", lambda: (self.pg.test_read_hashtag_trends(), self.mg.test_read_hashtag_trends()))
        ]
        
        for test_name, test_func in read_tests:
            try:
                pg_result, mongo_result = test_func()
                crud_results[f"READ: {test_name}"] = {
                    "postgresql": pg_result["time"],
                    "mongodb": mongo_result["time"]
                }
                print(f"    ✅ {test_name}")
            except Exception as e:
                print(f"    ❌ {test_name}: {e}")
                crud_results[f"READ: {test_name}"] = {
                    "postgresql": 0.0,
                    "mongodb": 0.0
                }
        
        # ========== CREATE TESTS ==========
        print("  ✏️  Testy CREATE...")
        
        create_tests = [
            ("New Tweet with New User", lambda: (self.pg.test_create(sample_row), self.mg.test_create(sample_row))),
            ("Tweet with Existing User", lambda: (self.pg.test_create_with_existing_user(sample_row), self.mg.test_create_with_existing_user(sample_row))),
            ("Tweet with Many Hashtags", lambda: (self.pg.test_create_with_many_hashtags(sample_row, num_hashtags=10), self.mg.test_create_with_many_hashtags(sample_row, num_hashtags=10))),
            ("Batch Insert", lambda: (self.pg.test_batch_insert(df_sample.head(100).to_dict('records'), batch_size=50), self.mg.test_batch_insert(df_sample.head(100).to_dict('records'), batch_size=50)))
        ]
        
        for test_name, test_func in create_tests:
            try:
                pg_result, mongo_result = test_func()
                crud_results[f"CREATE: {test_name}"] = {
                    "postgresql": pg_result["time"],
                    "mongodb": mongo_result["time"]
                }
                print(f"    ✅ {test_name}")
            except Exception as e:
                print(f"    ❌ {test_name}: {e}")
                crud_results[f"CREATE: {test_name}"] = {
                    "postgresql": 0.0,
                    "mongodb": 0.0
                }
        
        # ========== UPDATE TESTS ==========
        print("  🔄 Testy UPDATE...")
        
        update_tests = [
            ("Tweet Text", lambda: (self.pg.test_update(), self.mg.test_update())),
            ("User Data", lambda: (self.pg.test_update_user_data(), self.mg.test_update_user_data())),
            ("Bulk Update", lambda: (self.pg.test_bulk_update(days_ago=365), self.mg.test_bulk_update(days_ago=365)))
        ]
        
        for test_name, test_func in update_tests:
            try:
                pg_result, mongo_result = test_func()
                crud_results[f"UPDATE: {test_name}"] = {
                    "postgresql": pg_result["time"],
                    "mongodb": mongo_result["time"]
                }
                print(f"    ✅ {test_name}")
            except Exception as e:
                print(f"    ❌ {test_name}: {e}")
                crud_results[f"UPDATE: {test_name}"] = {
                    "postgresql": 0.0,
                    "mongodb": 0.0
                }
        
        # ========== DELETE TESTS ==========
        print("  🗑️  Testy DELETE...")
        
        delete_tests = [
            ("Single Tweet", lambda: (self.pg.test_delete(), self.mg.test_delete())),
            ("User's All Tweets", lambda: (self.pg.test_delete_user_tweets(), self.mg.test_delete_user_tweets()))
        ]
        
        for test_name, test_func in delete_tests:
            try:
                pg_result, mongo_result = test_func()
                crud_results[f"DELETE: {test_name}"] = {
                    "postgresql": pg_result["time"],
                    "mongodb": mongo_result["time"]
                }
                print(f"    ✅ {test_name}")
            except Exception as e:
                print(f"    ❌ {test_name}: {e}")
                crud_results[f"DELETE: {test_name}"] = {
                    "postgresql": 0.0,
                    "mongodb": 0.0
                }
        
        print("  ✅ Testy CRUD zakończone!")
        return crud_results
    
    def run_full_benchmark(self):
        """Przeprowadź pełny benchmark dla wszystkich zbiorów danych."""
        print("🚀 Rozpoczynanie pełnego benchmarku...")
        
        benchmark_start = time.perf_counter()
        
        # Definicja zbiorów danych
        datasets = [
            ("10K", "data/Bitcoin_tweets_10k.csv", None),
            ("100K", "data/Bitcoin_tweets_100k.csv", None),
            ("1M", "data/Bitcoin_tweets_1M.csv", 1000000)  # Limit dla bezpieczeństwa
        ]
        
        # Przeprowadź eksperymenty dla każdego zbioru
        for dataset_name, csv_path, sample_size in datasets:
            try:
                self.run_dataset_experiment(dataset_name, csv_path, sample_size)
            except Exception as e:
                print(f"❌ Błąd w eksperymencie {dataset_name}: {e}")
                continue
        
        benchmark_total = time.perf_counter() - benchmark_start
        
        print(f"\n🎉 Pełny benchmark zakończony w {benchmark_total:.2f}s")
        
        # Zapisz wyniki do pliku
        self.save_results()
        
        # Wygeneruj raporty
        self.generate_reports()
        
        return self.results
    
    def save_results(self):
        """Zapisz wyniki do pliku JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Wyniki zapisane do: {filename}")
    
    def generate_reports(self):
        """Wygeneruj tabele wyników i wykresy."""
        print("\n📊 Generowanie raportów...")
        
        # Generuj tabele
        self.generate_summary_table()
        self.generate_detailed_tables()
        
        # Generuj wykresy
        self.generate_load_time_chart()
        self.generate_crud_performance_charts()
        self.generate_scalability_analysis()
        
        print("✅ Raporty wygenerowane!")
    
    def generate_summary_table(self):
        """Wygeneruj tabelę podsumowującą."""
        print("\n📋 TABELA PODSUMOWUJĄCA")
        print("="*100)
        
        # Nagłówek tabeli
        header = f"{'Dataset':<10} {'Records':<10} {'PG Load (s)':<12} {'Mongo Load (s)':<15} {'PG Avg CRUD':<12} {'Mongo Avg CRUD':<15} {'Winner':<10}"
        print(header)
        print("-" * len(header))
        
        for dataset_name, data in self.results.items():
            records = data["dataset_info"]["records_clean"]
            pg_load = data["load_times"]["postgresql"]
            mongo_load = data["load_times"]["mongodb"]
            
            # Oblicz średnie czasy CRUD
            crud_results = data["crud_results"]
            pg_crud_times = [result["postgresql"] for result in crud_results.values() if result["postgresql"] > 0]
            mongo_crud_times = [result["mongodb"] for result in crud_results.values() if result["mongodb"] > 0]
            
            pg_avg_crud = np.mean(pg_crud_times) if pg_crud_times else 0
            mongo_avg_crud = np.mean(mongo_crud_times) if mongo_crud_times else 0
            
            # Określ zwycięzcę
            winner = "MongoDB" if mongo_avg_crud < pg_avg_crud else "PostgreSQL"
            
            row = f"{dataset_name:<10} {records:<10,} {pg_load:<12.4f} {mongo_load:<15.4f} {pg_avg_crud:<12.6f} {mongo_avg_crud:<15.6f} {winner:<10}"
            print(row)
        
        print("="*100)
    
    def generate_detailed_tables(self):
        """Wygeneruj szczegółowe tabele dla każdego zbioru danych."""
        for dataset_name, data in self.results.items():
            print(f"\n📊 SZCZEGÓŁOWE WYNIKI - {dataset_name}")
            print("="*120)
            
            header = f"{'Test Name':<40} {'PostgreSQL (s)':<15} {'MongoDB (s)':<15} {'Ratio (PG/Mongo)':<18} {'Winner':<15}"
            print(header)
            print("-" * len(header))
            
            crud_results = data["crud_results"]
            for test_name, result in crud_results.items():
                pg_time = result["postgresql"]
                mongo_time = result["mongodb"]
                
                if mongo_time > 0 and pg_time > 0:
                    ratio = pg_time / mongo_time
                    winner = "MongoDB" if ratio > 1 else "PostgreSQL"
                    winner_text = f"{winner} ({ratio:.2f}x)" if ratio != 1 else "Tie"
                else:
                    ratio = 0
                    winner_text = "N/A"
                
                row = f"{test_name:<40} {pg_time:<15.6f} {mongo_time:<15.6f} {ratio:<18.2f} {winner_text:<15}"
                print(row)
            
            print("="*120)
    
    def generate_load_time_chart(self):
        """Wygeneruj wykres czasów ładowania danych."""
        datasets = list(self.results.keys())
        pg_times = [self.results[ds]["load_times"]["postgresql"] for ds in datasets]
        mongo_times = [self.results[ds]["load_times"]["mongodb"] for ds in datasets]
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        x = np.arange(len(datasets))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, pg_times, width, label='PostgreSQL', alpha=0.8)
        bars2 = ax.bar(x + width/2, mongo_times, width, label='MongoDB', alpha=0.8)
        
        ax.set_xlabel('Rozmiar zbioru danych')
        ax.set_ylabel('Czas ładowania (sekundy)')
        ax.set_title('Porównanie czasów ładowania danych')
        ax.set_xticks(x)
        ax.set_xticklabels(datasets)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Dodaj wartości na słupkach
        for bar in bars1:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.2f}s', ha='center', va='bottom')
        
        for bar in bars2:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.2f}s', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('load_times_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("💾 Wykres czasów ładowania zapisany jako: load_times_comparison.png")
    
    def generate_crud_performance_charts(self):
        """Wygeneruj wykresy wydajności operacji CRUD."""
        # Grupuj operacje według typu
        operation_types = ['READ', 'CREATE', 'UPDATE', 'DELETE']
        
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        axes = axes.flatten()
        
        for idx, op_type in enumerate(operation_types):
            ax = axes[idx]
            
            # Zbierz dane dla danego typu operacji
            datasets = list(self.results.keys())
            pg_times = []
            mongo_times = []
            test_names = []
            
            # Użyj pierwszego zbioru danych jako referencji dla nazw testów
            first_dataset = list(self.results.keys())[0]
            crud_results = self.results[first_dataset]["crud_results"]
            
            for test_name, result in crud_results.items():
                if test_name.startswith(op_type):
                    test_names.append(test_name.replace(f"{op_type}: ", ""))
                    
                    # Zbierz średnie czasy dla wszystkich zbiorów danych
                    pg_avg = np.mean([self.results[ds]["crud_results"][test_name]["postgresql"] 
                                     for ds in datasets if test_name in self.results[ds]["crud_results"]])
                    mongo_avg = np.mean([self.results[ds]["crud_results"][test_name]["mongodb"] 
                                        for ds in datasets if test_name in self.results[ds]["crud_results"]])
                    
                    pg_times.append(pg_avg)
                    mongo_times.append(mongo_avg)
            
            if test_names:  # Jeśli są dane dla tego typu operacji
                x = np.arange(len(test_names))
                width = 0.35
                
                bars1 = ax.bar(x - width/2, pg_times, width, label='PostgreSQL', alpha=0.8)
                bars2 = ax.bar(x + width/2, mongo_times, width, label='MongoDB', alpha=0.8)
                
                ax.set_xlabel('Typ testu')
                ax.set_ylabel('Średni czas (sekundy)')
                ax.set_title(f'Wydajność operacji {op_type}')
                ax.set_xticks(x)
                ax.set_xticklabels(test_names, rotation=45, ha='right')
                ax.legend()
                ax.grid(True, alpha=0.3)
                
                # Dodaj wartości na słupkach
                for bar in bars1:
                    height = bar.get_height()
                    if height > 0:
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                               f'{height:.4f}', ha='center', va='bottom', fontsize=8)
                
                for bar in bars2:
                    height = bar.get_height()
                    if height > 0:
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                               f'{height:.4f}', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('crud_performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("💾 Wykres wydajności CRUD zapisany jako: crud_performance_comparison.png")
    
    def generate_scalability_analysis(self):
        """Wygeneruj analizę skalowalności."""
        datasets = list(self.results.keys())
        record_counts = [self.results[ds]["dataset_info"]["records_clean"] for ds in datasets]
        
        # Wybierz kilka kluczowych operacji do analizy skalowalności
        key_operations = [
            "READ: Count All Tweets",
            "READ: Recent Tweets", 
            "CREATE: New Tweet with New User",
            "UPDATE: Tweet Text"
        ]
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        axes = axes.flatten()
        
        for idx, operation in enumerate(key_operations):
            if idx >= len(axes):
                break
                
            ax = axes[idx]
            
            pg_times = []
            mongo_times = []
            
            for dataset in datasets:
                if operation in self.results[dataset]["crud_results"]:
                    pg_times.append(self.results[dataset]["crud_results"][operation]["postgresql"])
                    mongo_times.append(self.results[dataset]["crud_results"][operation]["mongodb"])
                else:
                    pg_times.append(0)
                    mongo_times.append(0)
            
            ax.plot(record_counts, pg_times, 'o-', label='PostgreSQL', linewidth=2, markersize=8)
            ax.plot(record_counts, mongo_times, 's-', label='MongoDB', linewidth=2, markersize=8)
            
            ax.set_xlabel('Liczba rekordów')
            ax.set_ylabel('Czas wykonania (sekundy)')
            ax.set_title(f'Skalowalność: {operation.replace("READ: ", "").replace("CREATE: ", "").replace("UPDATE: ", "")}')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.set_xscale('log')
            ax.set_yscale('log')
        
        plt.tight_layout()
        plt.savefig('scalability_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("💾 Analiza skalowalności zapisana jako: scalability_analysis.png")
    
    def close(self):
        """Zamknij połączenia z bazami danych."""
        try:
            self.pg.close()
            self.mg.close()
            print("✅ Połączenia z bazami danych zamknięte")
        except Exception as e:
            print(f"⚠️  Błąd podczas zamykania połączeń: {e}")


def main():
    """Główna funkcja uruchamiająca benchmark."""
    runner = None
    try:
        # Utwórz runner
        runner = BenchmarkRunner()
        
        # Przeprowadź pełny benchmark
        results = runner.run_full_benchmark()
        
        print(f"\n🎉 Benchmark zakończony pomyślnie!")
        print(f"📊 Przetestowano {len(results)} zbiorów danych")
        print(f"📁 Wyniki zapisane w plikach JSON i PNG")
        
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark przerwany przez użytkownika")
    except Exception as e:
        print(f"\n❌ Błąd podczas benchmarku: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if runner:
            runner.close()


if __name__ == "__main__":
    main()
