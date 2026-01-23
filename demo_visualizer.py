#!/usr/bin/env python3
"""
Demo wizualizator z przykładowymi danymi - pokazuje poprawione wykresy.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

def create_demo_data():
    """Stwórz przykładowe dane do demonstracji."""
    return {
        "10K": {
            "dataset_info": {
                "records": 10000,
                "clean_time": 0.08,
                "total_time": 20.4
            },
            "load_times": {
                "postgresql": 16.18,
                "mongodb": 0.36
            },
            "crud_results": {
                "READ: Count All Tweets": {"postgresql": 0.001625, "mongodb": 0.003505},
                "READ: Recent Tweets": {"postgresql": 0.003210, "mongodb": 0.000994},
                "READ: Search by Hashtag": {"postgresql": 0.008373, "mongodb": 0.001290},
                "READ: User Statistics": {"postgresql": 0.005668, "mongodb": 0.011591},
                "CREATE: New Tweet with New User": {"postgresql": 0.002447, "mongodb": 0.000582},
                "CREATE: Tweet with Existing User": {"postgresql": 0.001920, "mongodb": 0.000507},
                "UPDATE: Tweet Text": {"postgresql": 0.000730, "mongodb": 0.000608},
                "UPDATE: User Data": {"postgresql": 0.000766, "mongodb": 0.000363},
                "DELETE: Single Tweet": {"postgresql": 0.000787, "mongodb": 0.000371}
            }
        },
        "100K": {
            "dataset_info": {
                "records": 100000,
                "clean_time": 0.80,
                "total_time": 157.9
            },
            "load_times": {
                "postgresql": 0.0,  # timeout
                "mongodb": 3.91
            },
            "crud_results": {
                "READ: Count All Tweets": {"postgresql": 0.006763, "mongodb": 0.028788},
                "READ: Recent Tweets": {"postgresql": 0.015991, "mongodb": 0.001108},
                "READ: Search by Hashtag": {"postgresql": 0.053142, "mongodb": 0.001400},
                "READ: User Statistics": {"postgresql": 0.030318, "mongodb": 0.081867},
                "CREATE: New Tweet with New User": {"postgresql": 0.002760, "mongodb": 0.000838},
                "CREATE: Tweet with Existing User": {"postgresql": 0.002135, "mongodb": 0.000523},
                "UPDATE: Tweet Text": {"postgresql": 0.000889, "mongodb": 0.000535},
                "UPDATE: User Data": {"postgresql": 0.000668, "mongodb": 0.000324},
                "DELETE: Single Tweet": {"postgresql": 0.000698, "mongodb": 0.000376}
            }
        },
        "1M": {
            "dataset_info": {
                "records": 1000000,
                "clean_time": 10.20,
                "total_time": 988.9
            },
            "load_times": {
                "postgresql": 0.0,  # timeout
                "mongodb": 42.19
            },
            "crud_results": {
                "READ: Count All Tweets": {"postgresql": 0.050757, "mongodb": 0.350007},
                "READ: Recent Tweets": {"postgresql": 0.096810, "mongodb": 0.001642},
                "READ: Search by Hashtag": {"postgresql": 0.557717, "mongodb": 0.003018},
                "READ: User Statistics": {"postgresql": 0.234308, "mongodb": 1.153295},
                "CREATE: New Tweet with New User": {"postgresql": 0.003574, "mongodb": 0.001008},
                "CREATE: Tweet with Existing User": {"postgresql": 0.002006, "mongodb": 0.000483},
                "UPDATE: Tweet Text": {"postgresql": 0.001014, "mongodb": 0.000737},
                "UPDATE: User Data": {"postgresql": 0.000766, "mongodb": 0.000448},
                "DELETE: Single Tweet": {"postgresql": 0.012632, "mongodb": 0.002133}
            }
        }
    }

def setup_plot_style():
    """Konfiguracja stylu wykresów."""
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    
    colors = {
        'PostgreSQL': '#3498db',
        'MongoDB': '#2ecc71'
    }
    return colors

def create_improved_data_processing_charts(results):
    """Poprawione wykresy czasów przetwarzania danych."""
    datasets = list(results.keys())
    records = [results[ds]["dataset_info"]["records"] for ds in datasets]
    clean_times = [results[ds]["dataset_info"]["clean_time"] for ds in datasets]
    pg_load_times = [results[ds]["load_times"]["postgresql"] for ds in datasets]
    mongo_load_times = [results[ds]["load_times"]["mongodb"] for ds in datasets]
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    colors = setup_plot_style()
    
    # 1. POPRAWIONY wykres przygotowania danych
    x = np.arange(len(datasets))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, clean_times, width, label='Czyszczenie danych (wspólne)', 
                   color='#e74c3c', alpha=0.8)
    
    # Szacowane czasy cache clearing
    cache_times = [0.01, 0.02, 0.05]
    bars2 = ax1.bar(x + width/2, cache_times, width, label='Cache clearing (szacowane)', 
                   color='#9b59b6', alpha=0.8)
    
    ax1.set_title('Czasy przygotowania danych', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Czas (sekundy)')
    ax1.set_xticks(x)
    ax1.set_xticklabels(datasets)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Wartości na słupkach
    for bar, time in zip(bars1, clean_times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(clean_times)*0.01,
               f'{time:.2f}s', ha='center', va='bottom', fontweight='bold')
    
    for bar, time in zip(bars2, cache_times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(cache_times)*0.01,
               f'{time:.3f}s', ha='center', va='bottom', fontweight='bold')
    
    # 2. Porównanie czasów ładowania
    bars1 = ax2.bar(x - width/2, pg_load_times, width, label='PostgreSQL', 
                   color=colors['PostgreSQL'], alpha=0.8)
    bars2 = ax2.bar(x + width/2, mongo_load_times, width, label='MongoDB', 
                   color=colors['MongoDB'], alpha=0.8)
    
    ax2.set_title('Porównanie czasów ładowania danych', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Czas (sekundy)')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{ds}\n({records[i]:,})" for i, ds in enumerate(datasets)])
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Wartości na słupkach
    for bar, time in zip(bars1, pg_load_times):
        height = bar.get_height()
        if height > 0:
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}s', ha='center', va='bottom', fontweight='bold')
        else:
            ax2.text(bar.get_x() + bar.get_width()/2., 0.5,
                   'TIMEOUT', ha='center', va='bottom', fontweight='bold', color='red')
    
    for bar, time in zip(bars2, mongo_load_times):
        height = bar.get_height()
        if height > 0:
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                   f'{time:.1f}s', ha='center', va='bottom', fontweight='bold')
    
    # 3. Skalowalność ładowania (tylko MongoDB, bo PG ma timeout)
    valid_mongo_times = [t for t in mongo_load_times if t > 0]
    valid_records = [records[i] for i, t in enumerate(mongo_load_times) if t > 0]
    
    ax3.loglog(valid_records, valid_mongo_times, 's-', label='MongoDB', 
              linewidth=3, markersize=10, color=colors['MongoDB'])
    
    ax3.set_title('Skalowalność ładowania danych (MongoDB)', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Liczba rekordów')
    ax3.set_ylabel('Czas ładowania (sekundy)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Stosunek wydajności (tylko tam gdzie PG nie ma timeout)
    ratios = []
    valid_datasets = []
    for i, (pg, mongo) in enumerate(zip(pg_load_times, mongo_load_times)):
        if pg > 0 and mongo > 0:
            ratios.append(pg / mongo)
            valid_datasets.append(datasets[i])
    
    if ratios:
        bars = ax4.bar(valid_datasets, ratios, color='#f39c12', alpha=0.8)
        ax4.set_title('Stosunek wydajności ładowania (PG/MongoDB)', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Stosunek (wyżej = MongoDB szybszy)')
        ax4.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Równa wydajność')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        for bar, ratio in zip(bars, ratios):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + max(ratios)*0.01,
                   f'{ratio:.1f}x', ha='center', va='bottom', fontweight='bold')
    else:
        ax4.text(0.5, 0.5, 'PostgreSQL timeout\nw większości testów', 
                ha='center', va='center', transform=ax4.transAxes, 
                fontsize=14, fontweight='bold')
        ax4.set_title('Stosunek wydajności ładowania', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('demo_data_processing_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("💾 Demo wykres przetwarzania danych zapisany jako: demo_data_processing_analysis.png")

def create_improved_crud_charts(results):
    """Poprawione wykresy CRUD - słupkowe zamiast liniowych."""
    datasets = list(results.keys())
    records = [results[ds]["dataset_info"]["records"] for ds in datasets]
    colors = setup_plot_style()
    
    # Przykład dla operacji READ
    read_operations = [
        "READ: Count All Tweets",
        "READ: Recent Tweets", 
        "READ: Search by Hashtag",
        "READ: User Statistics"
    ]
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()
    
    fig.suptitle('Poprawione wykresy CRUD - Operacje READ (słupkowe)', 
                fontsize=16, fontweight='bold')
    
    for idx, operation in enumerate(read_operations):
        ax = axes[idx]
        
        # Zbierz dane
        pg_times = []
        mongo_times = []
        
        for dataset in datasets:
            crud_result = results[dataset]["crud_results"].get(operation, {})
            pg_time = crud_result.get("postgresql", 0.0) * 1000  # ms
            mongo_time = crud_result.get("mongodb", 0.0) * 1000  # ms
            
            pg_times.append(pg_time)
            mongo_times.append(mongo_time)
        
        # POPRAWIONY wykres słupkowy
        x_pos = np.arange(len(datasets))
        width = 0.35
        
        bars1 = ax.bar(x_pos - width/2, pg_times, width, label='PostgreSQL', 
                      color=colors['PostgreSQL'], alpha=0.8)
        bars2 = ax.bar(x_pos + width/2, mongo_times, width, label='MongoDB', 
                      color=colors['MongoDB'], alpha=0.8)
        
        # Formatowanie
        op_name = operation.split(': ', 1)[1]
        ax.set_title(op_name, fontsize=12, fontweight='bold')
        ax.set_xlabel('Rozmiar zbioru danych')
        ax.set_ylabel('Czas (milisekundy)')
        ax.set_xticks(x_pos)
        ax.set_xticklabels([f"{ds}\n({records[i]:,})" for i, ds in enumerate(datasets)])
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Wartości na słupkach
        for bar, time in zip(bars1, pg_times):
            if time > 0:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{time:.1f}', ha='center', va='bottom', 
                       fontsize=9, fontweight='bold')
        
        for bar, time in zip(bars2, mongo_times):
            if time > 0:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{time:.1f}', ha='center', va='bottom', 
                       fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('demo_read_operations_improved.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("💾 Demo wykres operacji READ zapisany jako: demo_read_operations_improved.png")

def main():
    """Demo głównej funkcji."""
    print("📊 Demo poprawionych wykresów")
    print("="*50)
    
    # Stwórz przykładowe dane
    results = create_demo_data()
    
    # Konfiguracja
    colors = setup_plot_style()
    
    print("🎨 Generowanie poprawionych wykresów...")
    
    print("1. Poprawiony wykres przetwarzania danych...")
    create_improved_data_processing_charts(results)
    
    print("2. Poprawione wykresy CRUD (słupkowe)...")
    create_improved_crud_charts(results)
    
    print("\n🎉 Demo zakończone!")
    print("📁 Wygenerowane pliki:")
    print("  - demo_data_processing_analysis.png")
    print("  - demo_read_operations_improved.png")
    
    print("\n✨ Główne poprawki:")
    print("  ✅ Wykres czyszczenia danych pokazuje porównanie z cache clearing")
    print("  ✅ Wykresy CRUD są teraz słupkowe zamiast liniowych")
    print("  ✅ Wszystkie wykresy mają wartości na słupkach")
    print("  ✅ Lepsze formatowanie i czytelność")

if __name__ == "__main__":
    main()
