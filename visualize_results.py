#!/usr/bin/env python3
"""
Skrypt do wizualizacji wyników benchmarku PostgreSQL vs MongoDB.
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from pathlib import Path
import glob

def load_latest_results():
    """Załaduj najnowsze wyniki benchmarku."""
    # Znajdź najnowszy plik wyników
    result_files = glob.glob("simple_benchmark_results_*.json")
    if not result_files:
        print("❌ Nie znaleziono plików wyników!")
        return None
    
    latest_file = max(result_files)
    print(f"📁 Ładowanie wyników z: {latest_file}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_load_time_chart(results):
    """Utwórz wykres czasów ładowania danych."""
    datasets = list(results.keys())
    records = [results[ds]["dataset_info"]["records"] for ds in datasets]
    pg_times = [results[ds]["load_times"]["postgresql"] for ds in datasets]
    mongo_times = [results[ds]["load_times"]["mongodb"] for ds in datasets]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Wykres słupkowy
    x = np.arange(len(datasets))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, pg_times, width, label='PostgreSQL', alpha=0.8, color='#3498db')
    bars2 = ax1.bar(x + width/2, mongo_times, width, label='MongoDB', alpha=0.8, color='#2ecc71')
    
    ax1.set_xlabel('Rozmiar zbioru danych')
    ax1.set_ylabel('Czas ładowania (sekundy)')
    ax1.set_title('Porównanie czasów ładowania danych')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{ds}\n({records[i]:,} rekordów)" for i, ds in enumerate(datasets)])
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Dodaj wartości na słupkach
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}s', ha='center', va='bottom', fontweight='bold')
    
    for bar in bars2:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}s', ha='center', va='bottom', fontweight='bold')
    
    # Wykres skalowalności (log-log)
    ax2.loglog(records, pg_times, 'o-', label='PostgreSQL', linewidth=3, markersize=10, color='#3498db')
    ax2.loglog(records, mongo_times, 's-', label='MongoDB', linewidth=3, markersize=10, color='#2ecc71')
    
    ax2.set_xlabel('Liczba rekordów')
    ax2.set_ylabel('Czas ładowania (sekundy)')
    ax2.set_title('Skalowalność ładowania danych (skala logarytmiczna)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Dodaj adnotacje z wartościami
    for i, (rec, pg_time, mongo_time) in enumerate(zip(records, pg_times, mongo_times)):
        ax2.annotate(f'{pg_time:.1f}s', (rec, pg_time), xytext=(5, 5), 
                    textcoords='offset points', fontsize=9, color='#3498db')
        ax2.annotate(f'{mongo_time:.1f}s', (rec, mongo_time), xytext=(5, -15), 
                    textcoords='offset points', fontsize=9, color='#2ecc71')
    
    plt.tight_layout()
    plt.savefig('load_times_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("💾 Wykres czasów ładowania zapisany jako: load_times_comparison.png")

def create_crud_performance_chart(results):
    """Utwórz wykres wydajności operacji CRUD."""
    # Przygotuj dane
    crud_data = []
    
    for dataset_name, data in results.items():
        records = data["dataset_info"]["records"]
        crud_results = data["crud_results"]
        
        for test_name, times in crud_results.items():
            operation_type = test_name.split(":")[0]
            test_desc = test_name.split(":")[1].strip()
            
            crud_data.append({
                'Dataset': f"{dataset_name}\n({records:,})",
                'Operation': operation_type,
                'Test': test_desc,
                'PostgreSQL': times["postgresql"] * 1000,  # Konwersja na milisekundy
                'MongoDB': times["mongodb"] * 1000,
                'Ratio': times["postgresql"] / times["mongodb"] if times["mongodb"] > 0 else 0
            })
    
    df = pd.DataFrame(crud_data)
    
    # Grupuj według typu operacji
    operation_types = df['Operation'].unique()
    
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    axes = axes.flatten()
    
    colors = {'PostgreSQL': '#3498db', 'MongoDB': '#2ecc71'}
    
    for idx, op_type in enumerate(operation_types):
        if idx >= len(axes):
            break
            
        ax = axes[idx]
        op_data = df[df['Operation'] == op_type]
        
        # Przygotuj dane do wykresu
        tests = op_data['Test'].unique()
        datasets = op_data['Dataset'].unique()
        
        x = np.arange(len(tests))
        width = 0.35
        
        # Oblicz średnie dla każdego testu
        pg_means = []
        mongo_means = []
        
        for test in tests:
            test_data = op_data[op_data['Test'] == test]
            pg_means.append(test_data['PostgreSQL'].mean())
            mongo_means.append(test_data['MongoDB'].mean())
        
        bars1 = ax.bar(x - width/2, pg_means, width, label='PostgreSQL', 
                      alpha=0.8, color=colors['PostgreSQL'])
        bars2 = ax.bar(x + width/2, mongo_means, width, label='MongoDB', 
                      alpha=0.8, color=colors['MongoDB'])
        
        ax.set_xlabel('Typ testu')
        ax.set_ylabel('Średni czas (milisekundy)')
        ax.set_title(f'Wydajność operacji {op_type}')
        ax.set_xticks(x)
        ax.set_xticklabels(tests, rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Dodaj wartości na słupkach
        for bar in bars1:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.2f}', ha='center', va='bottom', fontsize=8)
        
        for bar in bars2:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.2f}', ha='center', va='bottom', fontsize=8)
    
    # Usuń puste subploty
    for idx in range(len(operation_types), len(axes)):
        fig.delaxes(axes[idx])
    
    plt.tight_layout()
    plt.savefig('crud_performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("💾 Wykres wydajności CRUD zapisany jako: crud_performance_comparison.png")

def create_summary_table(results):
    """Utwórz tabelę podsumowującą wyniki."""
    print("\n📋 SZCZEGÓŁOWA TABELA WYNIKÓW")
    print("="*120)
    
    # Nagłówek
    header = f"{'Dataset':<10} {'Records':<12} {'PG Load (s)':<12} {'Mongo Load (s)':<15} {'Load Ratio':<12} {'PG CRUD Avg':<15} {'Mongo CRUD Avg':<18} {'CRUD Winner':<15}"
    print(header)
    print("-" * len(header))
    
    for dataset_name, data in results.items():
        records = data["dataset_info"]["records"]
        pg_load = data["load_times"]["postgresql"]
        mongo_load = data["load_times"]["mongodb"]
        load_ratio = pg_load / mongo_load if mongo_load > 0 else 0
        
        # Oblicz średnie czasy CRUD
        crud_results = data["crud_results"]
        pg_crud_times = [result["postgresql"] for result in crud_results.values() if result["postgresql"] > 0]
        mongo_crud_times = [result["mongodb"] for result in crud_results.values() if result["mongodb"] > 0]
        
        pg_avg_crud = np.mean(pg_crud_times) if pg_crud_times else 0
        mongo_avg_crud = np.mean(mongo_crud_times) if mongo_crud_times else 0
        
        crud_winner = "MongoDB" if mongo_avg_crud < pg_avg_crud else "PostgreSQL"
        
        row = f"{dataset_name:<10} {records:<12,} {pg_load:<12.2f} {mongo_load:<15.2f} {load_ratio:<12.1f}x {pg_avg_crud:<15.6f} {mongo_avg_crud:<18.6f} {crud_winner:<15}"
        print(row)
    
    print("="*120)

def create_detailed_comparison_table(results):
    """Utwórz szczegółową tabelę porównawczą dla każdego testu."""
    for dataset_name, data in results.items():
        print(f"\n📊 SZCZEGÓŁOWE WYNIKI - {dataset_name} ({data['dataset_info']['records']:,} rekordów)")
        print("="*100)
        
        header = f"{'Test Name':<35} {'PostgreSQL (ms)':<18} {'MongoDB (ms)':<15} {'Ratio':<10} {'Winner':<15}"
        print(header)
        print("-" * len(header))
        
        crud_results = data["crud_results"]
        for test_name, result in crud_results.items():
            pg_time = result["postgresql"] * 1000  # Konwersja na ms
            mongo_time = result["mongodb"] * 1000
            
            if mongo_time > 0 and pg_time > 0:
                ratio = pg_time / mongo_time
                if ratio > 1.1:
                    winner = f"MongoDB ({ratio:.1f}x)"
                elif ratio < 0.9:
                    winner = f"PostgreSQL ({1/ratio:.1f}x)"
                else:
                    winner = "Podobne"
            else:
                ratio = 0
                winner = "N/A"
            
            row = f"{test_name:<35} {pg_time:<18.3f} {mongo_time:<15.3f} {ratio:<10.2f} {winner:<15}"
            print(row)
        
        print("="*100)

def main():
    """Główna funkcja wizualizacji."""
    print("📊 Generator wizualizacji wyników benchmarku")
    print("="*60)
    
    # Załaduj wyniki
    results = load_latest_results()
    if not results:
        return
    
    print(f"✅ Załadowano wyniki dla {len(results)} zbiorów danych")
    
    # Konfiguracja wykresów
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    
    # Generuj wizualizacje
    print("\n🎨 Generowanie wykresów...")
    
    create_load_time_chart(results)
    create_crud_performance_chart(results)
    
    # Generuj tabele
    print("\n📋 Generowanie tabel...")
    create_summary_table(results)
    create_detailed_comparison_table(results)
    
    print(f"\n🎉 Wizualizacja zakończona!")
    print(f"📁 Wygenerowane pliki:")
    print(f"  - load_times_comparison.png")
    print(f"  - crud_performance_comparison.png")

if __name__ == "__main__":
    main()
