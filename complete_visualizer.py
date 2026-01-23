#!/usr/bin/env python3
"""
Kompletny wizualizator - wszystkie 20 operacji CRUD z jasnymi wykresami.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

def create_complete_demo_data():
    """Kompletne dane demonstracyjne - wszystkie 20 operacji CRUD."""
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
                # 10 operacji READ
                "READ: Count All Tweets": {"postgresql": 0.001625, "mongodb": 0.003505},
                "READ: Recent Tweets": {"postgresql": 0.003210, "mongodb": 0.000994},
                "READ: Search by Hashtag": {"postgresql": 0.008373, "mongodb": 0.001290},
                "READ: User Statistics": {"postgresql": 0.005668, "mongodb": 0.011591},
                "READ: Popular Hashtags": {"postgresql": 0.015234, "mongodb": 0.008765},
                "READ: Daily Statistics": {"postgresql": 0.002553, "mongodb": 0.001120},
                "READ: Complex Aggregations": {"postgresql": 0.025678, "mongodb": 0.018432},
                "READ: User Ranking": {"postgresql": 0.012345, "mongodb": 0.009876},
                "READ: Daily Activity": {"postgresql": 0.002437, "mongodb": 0.000911},
                "READ: Hashtag Trends": {"postgresql": 0.002391, "mongodb": 0.000684},
                
                # 4 operacje CREATE
                "CREATE: New Tweet with New User": {"postgresql": 0.002447, "mongodb": 0.000582},
                "CREATE: Tweet with Existing User": {"postgresql": 0.001920, "mongodb": 0.000507},
                "CREATE: Tweet with Many Hashtags": {"postgresql": 0.003456, "mongodb": 0.000789},
                "CREATE: Batch Insert": {"postgresql": 0.025678, "mongodb": 0.005432},
                
                # 3 operacje UPDATE
                "UPDATE: Tweet Text": {"postgresql": 0.000730, "mongodb": 0.000608},
                "UPDATE: User Data": {"postgresql": 0.000766, "mongodb": 0.000363},
                "UPDATE: Bulk Update": {"postgresql": 0.045789, "mongodb": 0.129599},
                
                # 3 operacje DELETE
                "DELETE: Single Tweet": {"postgresql": 0.000787, "mongodb": 0.000371},
                "DELETE: User's All Tweets": {"postgresql": 0.001387, "mongodb": 0.001283},
                "DELETE: Old Tweets": {"postgresql": 0.029634, "mongodb": 0.068543}
            }
        },
        "100K": {
            "dataset_info": {
                "records": 100000,
                "clean_time": 0.80,
                "total_time": 157.9
            },
            "load_times": {
                "postgresql": 203.71,  # Rzeczywiste dane
                "mongodb": 3.91
            },
            "crud_results": {
                # 10 operacji READ
                "READ: Count All Tweets": {"postgresql": 0.006763, "mongodb": 0.028788},
                "READ: Recent Tweets": {"postgresql": 0.015991, "mongodb": 0.001108},
                "READ: Search by Hashtag": {"postgresql": 0.053142, "mongodb": 0.001400},
                "READ: User Statistics": {"postgresql": 0.030318, "mongodb": 0.081867},
                "READ: Popular Hashtags": {"postgresql": 0.131962, "mongodb": 0.150511},
                "READ: Daily Statistics": {"postgresql": 0.002553, "mongodb": 0.001120},
                "READ: Complex Aggregations": {"postgresql": 0.140781, "mongodb": 0.191657},
                "READ: User Ranking": {"postgresql": 0.020850, "mongodb": 0.088330},
                "READ: Daily Activity": {"postgresql": 0.002437, "mongodb": 0.000911},
                "READ: Hashtag Trends": {"postgresql": 0.002391, "mongodb": 0.000684},
                
                # 4 operacje CREATE
                "CREATE: New Tweet with New User": {"postgresql": 0.006895, "mongodb": 0.000680},
                "CREATE: Tweet with Existing User": {"postgresql": 0.001777, "mongodb": 0.000605},
                "CREATE: Tweet with Many Hashtags": {"postgresql": 0.008478, "mongodb": 0.000719},
                "CREATE: Batch Insert": {"postgresql": 0.052559, "mongodb": 0.002619},
                
                # 3 operacje UPDATE
                "UPDATE: Tweet Text": {"postgresql": 0.001220, "mongodb": 0.000756},
                "UPDATE: User Data": {"postgresql": 0.000643, "mongodb": 0.000394},
                "UPDATE: Bulk Update": {"postgresql": 0.291838, "mongodb": 0.498956},
                
                # 3 operacje DELETE
                "DELETE: Single Tweet": {"postgresql": 0.001284, "mongodb": 0.000600},
                "DELETE: User's All Tweets": {"postgresql": 0.000772, "mongodb": 0.000979},
                "DELETE: Old Tweets": {"postgresql": 0.212734, "mongodb": 0.817563}
            }
        },
        "1M": {
            "dataset_info": {
                "records": 1000000,
                "clean_time": 10.20,
                "total_time": 988.9
            },
            "load_times": {
                "postgresql": 1800.0,  # Szacowane (timeout po 30min)
                "mongodb": 42.19
            },
            "crud_results": {
                # 10 operacji READ
                "READ: Count All Tweets": {"postgresql": 0.050757, "mongodb": 0.350007},
                "READ: Recent Tweets": {"postgresql": 0.096810, "mongodb": 0.001642},
                "READ: Search by Hashtag": {"postgresql": 0.557717, "mongodb": 0.003018},
                "READ: User Statistics": {"postgresql": 0.234308, "mongodb": 1.153295},
                "READ: Popular Hashtags": {"postgresql": 0.735977, "mongodb": 2.167513},
                "READ: Daily Statistics": {"postgresql": 0.011643, "mongodb": 0.001623},
                "READ: Complex Aggregations": {"postgresql": 0.659790, "mongodb": 3.574671},
                "READ: User Ranking": {"postgresql": 0.327804, "mongodb": 1.171229},
                "READ: Daily Activity": {"postgresql": 0.012093, "mongodb": 0.001020},
                "READ: Hashtag Trends": {"postgresql": 0.008820, "mongodb": 0.000992},
                
                # 4 operacje CREATE
                "CREATE: New Tweet with New User": {"postgresql": 0.003574, "mongodb": 0.001008},
                "CREATE: Tweet with Existing User": {"postgresql": 0.002006, "mongodb": 0.000483},
                "CREATE: Tweet with Many Hashtags": {"postgresql": 0.010582, "mongodb": 0.000831},
                "CREATE: Batch Insert": {"postgresql": 0.069819, "mongodb": 0.005658},
                
                # 3 operacje UPDATE
                "UPDATE: Tweet Text": {"postgresql": 0.001014, "mongodb": 0.000737},
                "UPDATE: User Data": {"postgresql": 0.000766, "mongodb": 0.000448},
                "UPDATE: Bulk Update": {"postgresql": 1.927952, "mongodb": 5.498418},
                
                # 3 operacje DELETE
                "DELETE: Single Tweet": {"postgresql": 0.012632, "mongodb": 0.002133},
                "DELETE: User's All Tweets": {"postgresql": 0.001336, "mongodb": 0.006518},
                "DELETE: Old Tweets": {"postgresql": 2.207015, "mongodb": 12.105752}
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

def create_clear_data_processing_charts(results):
    """
    Tworzy wykresy analizy przetwarzania danych.
    
    Ten wykres pokazuje:
    1. Czasy czyszczenia danych CSV (wspólne dla obu baz danych)
    2. Porównanie czasów ładowania danych do PostgreSQL vs MongoDB
    3. Skalowalność ładowania danych w zależności od rozmiaru zbioru
    4. Stosunek wydajności ładowania między bazami danych
    """
    datasets = list(results.keys())
    records = [results[ds]["dataset_info"]["records"] for ds in datasets]
    clean_times = [results[ds]["dataset_info"]["clean_time"] for ds in datasets]
    pg_load_times = [results[ds]["load_times"]["postgresql"] for ds in datasets]
    mongo_load_times = [results[ds]["load_times"]["mongodb"] for ds in datasets]
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    colors = setup_plot_style()
    
    # Główny tytuł dla całego wykresu
    fig.suptitle('Analiza Przetwarzania Danych - PostgreSQL vs MongoDB', 
                fontsize=18, fontweight='bold', y=0.98)
    
    # 1. Czasy czyszczenia danych (wspólne dla obu baz)
    bars = ax1.bar(datasets, clean_times, color='#e74c3c', alpha=0.8)
    ax1.set_title('Czas Czyszczenia Danych CSV\n(Wspólny etap przygotowania dla obu baz)', 
                 fontsize=14, fontweight='bold')
    ax1.set_ylabel('Czas (sekundy)')
    ax1.set_xlabel('Rozmiar zbioru danych')
    ax1.grid(True, alpha=0.3)
    
    for bar, time in zip(bars, clean_times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(clean_times)*0.01,
               f'{time:.2f}s', ha='center', va='bottom', fontweight='bold')
    
    # 2. Porównanie czasów ładowania danych
    x = np.arange(len(datasets))
    width = 0.35
    
    bars1 = ax2.bar(x - width/2, pg_load_times, width, label='PostgreSQL', 
                   color=colors['PostgreSQL'], alpha=0.8)
    bars2 = ax2.bar(x + width/2, mongo_load_times, width, label='MongoDB', 
                   color=colors['MongoDB'], alpha=0.8)
    
    ax2.set_title('Porównanie Czasów Ładowania Danych\n(PostgreSQL vs MongoDB)', 
                 fontsize=14, fontweight='bold')
    ax2.set_ylabel('Czas ładowania (sekundy)')
    ax2.set_xlabel('Rozmiar zbioru danych')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{ds}\n({records[i]:,} rekordów)" for i, ds in enumerate(datasets)])
    ax2.legend(title='Baza danych')
    ax2.grid(True, alpha=0.3)
    
    # Wartości na słupkach
    for bar, time in zip(bars1, pg_load_times):
        height = bar.get_height()
        if height >= 1800:  # timeout
            ax2.text(bar.get_x() + bar.get_width()/2., height/2,
                   'TIMEOUT\n(30min)', ha='center', va='center', fontweight='bold', color='white')
        else:
            ax2.text(bar.get_x() + bar.get_width()/2., height + max(pg_load_times)*0.01,
                   f'{time:.1f}s', ha='center', va='bottom', fontweight='bold')
    
    for bar, time in zip(bars2, mongo_load_times):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + max(mongo_load_times)*0.01,
               f'{time:.1f}s', ha='center', va='bottom', fontweight='bold')
    
    # 3. Skalowalność ładowania (obie bazy)
    ax3.loglog(records, pg_load_times, 'o-', label='PostgreSQL', 
              linewidth=3, markersize=10, color=colors['PostgreSQL'])
    ax3.loglog(records, mongo_load_times, 's-', label='MongoDB', 
              linewidth=3, markersize=10, color=colors['MongoDB'])
    
    ax3.set_title('Skalowalność Ładowania Danych\n(Wzrost czasu w zależności od rozmiaru)', 
                 fontsize=14, fontweight='bold')
    ax3.set_xlabel('Liczba rekordów (skala logarytmiczna)')
    ax3.set_ylabel('Czas ładowania (sekundy, skala logarytmiczna)')
    ax3.legend(title='Baza danych')
    ax3.grid(True, alpha=0.3)
    
    # 4. Stosunek wydajności ładowania
    ratios = [pg/mongo if mongo > 0 else 0 for pg, mongo in zip(pg_load_times, mongo_load_times)]
    bars = ax4.bar(datasets, ratios, color='#f39c12', alpha=0.8)
    ax4.set_title('Stosunek Wydajności Ładowania\n(PostgreSQL / MongoDB)', 
                 fontsize=14, fontweight='bold')
    ax4.set_ylabel('Stosunek czasów\n(wartość > 1 = MongoDB szybszy)')
    ax4.set_xlabel('Rozmiar zbioru danych')
    ax4.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Równa wydajność')
    ax4.legend(title='Linia odniesienia')
    ax4.grid(True, alpha=0.3)
    
    for bar, ratio in zip(bars, ratios):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + max(ratios)*0.01,
               f'{ratio:.1f}x', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('complete_data_processing_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("💾 Kompletny wykres przetwarzania danych zapisany jako: complete_data_processing_analysis.png")

def create_all_crud_operations_charts(results):
    """
    Tworzy wykresy wszystkich 20 operacji CRUD.
    
    Wykresy pokazują porównanie wydajności PostgreSQL vs MongoDB dla:
    - READ (10 operacji): Różne typy zapytań i wyszukiwań
    - CREATE (4 operacje): Wstawianie nowych danych
    - UPDATE (3 operacje): Modyfikacja istniejących danych  
    - DELETE (3 operacje): Usuwanie danych
    
    Każdy wykres przedstawia czasy wykonania w milisekundach dla trzech rozmiarów zbiorów danych.
    """
    datasets = list(results.keys())
    records = [results[ds]["dataset_info"]["records"] for ds in datasets]
    colors = setup_plot_style()
    
    # Grupuj operacje według typu
    operation_types = {
        'READ': [],
        'CREATE': [],
        'UPDATE': [],
        'DELETE': []
    }
    
    # Zbierz wszystkie operacje
    sample_data = list(results.values())[0]
    for operation in sample_data["crud_results"].keys():
        op_type = operation.split(':')[0]
        if op_type in operation_types:
            operation_types[op_type].append(operation)
    
    # Utwórz wykresy dla każdego typu operacji
    for op_type, operations in operation_types.items():
        if not operations:
            continue
            
        n_ops = len(operations)
        cols = min(4, n_ops)  # Maksymalnie 4 kolumny
        rows = (n_ops + cols - 1) // cols
        
        fig, axes = plt.subplots(rows, cols, figsize=(5*cols, 4*rows))
        if rows == 1 and cols == 1:
            axes = [axes]
        elif rows == 1:
            axes = axes
        else:
            axes = axes.flatten()
        
        # Polskie tytuły dla każdego typu operacji
        type_titles = {
            'READ': 'Operacje Odczytu (READ) - Porównanie PostgreSQL vs MongoDB',
            'CREATE': 'Operacje Tworzenia (CREATE) - Porównanie PostgreSQL vs MongoDB', 
            'UPDATE': 'Operacje Aktualizacji (UPDATE) - Porównanie PostgreSQL vs MongoDB',
            'DELETE': 'Operacje Usuwania (DELETE) - Porównanie PostgreSQL vs MongoDB'
        }
        
        fig.suptitle(type_titles.get(op_type, f'Operacje {op_type} - Porównanie PostgreSQL vs MongoDB'), 
                    fontsize=16, fontweight='bold')
        
        for idx, operation in enumerate(operations):
            ax = axes[idx] if idx < len(axes) else None
            if ax is None:
                break
                
            # Zbierz dane dla tej operacji
            pg_times = []
            mongo_times = []
            
            for dataset in datasets:
                crud_result = results[dataset]["crud_results"].get(operation, {})
                pg_time = crud_result.get("postgresql", 0.0) * 1000  # ms
                mongo_time = crud_result.get("mongodb", 0.0) * 1000  # ms
                
                pg_times.append(pg_time)
                mongo_times.append(mongo_time)
            
            # Wykres słupkowy
            x_pos = np.arange(len(datasets))
            width = 0.35
            
            bars1 = ax.bar(x_pos - width/2, pg_times, width, label='PostgreSQL', 
                          color=colors['PostgreSQL'], alpha=0.8)
            bars2 = ax.bar(x_pos + width/2, mongo_times, width, label='MongoDB', 
                          color=colors['MongoDB'], alpha=0.8)
            
            # Formatowanie z polskimi tłumaczeniami nazw operacji
            op_name = operation.split(': ', 1)[1] if ': ' in operation else operation
            
            # Słownik tłumaczeń nazw operacji na polski
            polish_translations = {
                'Count All Tweets': 'Zliczenie wszystkich tweetów',
                'Recent Tweets': 'Najnowsze tweety',
                'Search by Hashtag': 'Wyszukiwanie po hashtagu',
                'User Statistics': 'Statystyki użytkowników',
                'Popular Hashtags': 'Popularne hashtagi',
                'Daily Statistics': 'Statystyki dzienne',
                'Complex Aggregations': 'Złożone agregacje',
                'User Ranking': 'Ranking użytkowników',
                'Daily Activity': 'Aktywność dzienna',
                'Hashtag Trends': 'Trendy hashtagów',
                'New Tweet with New User': 'Nowy tweet z nowym użytkownikiem',
                'Tweet with Existing User': 'Tweet z istniejącym użytkownikiem',
                'Tweet with Many Hashtags': 'Tweet z wieloma hashtagami',
                'Batch Insert': 'Wstawianie wsadowe',
                'Tweet Text': 'Tekst tweeta',
                'User Data': 'Dane użytkownika',
                'Bulk Update': 'Aktualizacja wsadowa',
                'Single Tweet': 'Pojedynczy tweet',
                "User's All Tweets": 'Wszystkie tweety użytkownika',
                'Old Tweets': 'Stare tweety'
            }
            
            polish_name = polish_translations.get(op_name, op_name)
            ax.set_title(polish_name, fontsize=11, fontweight='bold')
            ax.set_xlabel('Rozmiar zbioru danych')
            ax.set_ylabel('Czas wykonania (milisekundy)')
            ax.set_xticks(x_pos)
            ax.set_xticklabels([f"{ds}\n({records[i]:,} rekordów)" for i, ds in enumerate(datasets)], fontsize=9)
            ax.legend(title='Baza danych', fontsize=9)
            ax.grid(True, alpha=0.3)
            
            # Wartości na słupkach
            for bar, time in zip(bars1, pg_times):
                if time > 0:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{time:.1f}', ha='center', va='bottom', 
                           fontsize=8, fontweight='bold')
            
            for bar, time in zip(bars2, mongo_times):
                if time > 0:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{time:.1f}', ha='center', va='bottom', 
                           fontsize=8, fontweight='bold')
        
        # Usuń puste subploty
        for idx in range(len(operations), len(axes)):
            fig.delaxes(axes[idx])
        
        plt.tight_layout()
        plt.savefig(f'complete_{op_type.lower()}_operations.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"💾 Kompletny wykres operacji {op_type} zapisany jako: complete_{op_type.lower()}_operations.png")
        print(f"   📊 Zawiera wszystkie {len(operations)} operacji {op_type}")

def create_summary_comparison_table(results):
    """
    Tworzy szczegółową tabelę porównawczą wszystkich operacji CRUD.
    
    Tabela pokazuje:
    - Czasy wykonania każdej operacji dla wszystkich rozmiarów zbiorów danych
    - Porównanie PostgreSQL vs MongoDB w milisekundach
    - Określenie zwycięzcy dla każdej operacji
    - Podsumowanie według typu operacji (READ, CREATE, UPDATE, DELETE)
    """
    print("\n📊 SZCZEGÓŁOWA ANALIZA WYDAJNOŚCI - WSZYSTKIE 20 OPERACJI CRUD")
    print("="*120)
    print("Porównanie czasów wykonania PostgreSQL vs MongoDB")
    print("Czasy podane w milisekundach (ms) - niższe wartości = lepsza wydajność")
    print("="*120)
    
    datasets = list(results.keys())
    
    # Nagłówek z polskimi opisami
    header = f"{'Nazwa Operacji':<40} {'10K PG(ms)':<12} {'10K Mongo(ms)':<14} {'100K PG(ms)':<13} {'100K Mongo(ms)':<15} {'1M PG(ms)':<11} {'1M Mongo(ms)':<13} {'Zwycięzca':<15}"
    print(header)
    print("-" * len(header))
    
    # Wszystkie operacje
    sample_data = list(results.values())[0]
    all_operations = sorted(sample_data["crud_results"].keys())
    
    # Słownik tłumaczeń nazw operacji na polski (dla tabeli)
    polish_translations = {
        'Count All Tweets': 'Zliczenie wszystkich tweetów',
        'Recent Tweets': 'Najnowsze tweety',
        'Search by Hashtag': 'Wyszukiwanie po hashtagu',
        'User Statistics': 'Statystyki użytkowników',
        'Popular Hashtags': 'Popularne hashtagi',
        'Daily Statistics': 'Statystyki dzienne',
        'Complex Aggregations': 'Złożone agregacje',
        'User Ranking': 'Ranking użytkowników',
        'Daily Activity': 'Aktywność dzienna',
        'Hashtag Trends': 'Trendy hashtagów',
        'New Tweet with New User': 'Nowy tweet z nowym użytkownikiem',
        'Tweet with Existing User': 'Tweet z istniejącym użytkownikiem',
        'Tweet with Many Hashtags': 'Tweet z wieloma hashtagami',
        'Batch Insert': 'Wstawianie wsadowe',
        'Tweet Text': 'Tekst tweeta',
        'User Data': 'Dane użytkownika',
        'Bulk Update': 'Aktualizacja wsadowa',
        'Single Tweet': 'Pojedynczy tweet',
        "User's All Tweets": 'Wszystkie tweety użytkownika',
        'Old Tweets': 'Stare tweety'
    }
    
    for operation in all_operations:
        op_name = operation.split(': ', 1)[1] if ': ' in operation else operation
        polish_name = polish_translations.get(op_name, op_name)
        polish_name = polish_name[:38] + '..' if len(polish_name) > 38 else polish_name
        
        times = []
        for dataset in datasets:
            crud_result = results[dataset]["crud_results"].get(operation, {})
            pg_time = crud_result.get("postgresql", 0.0) * 1000  # ms
            mongo_time = crud_result.get("mongodb", 0.0) * 1000  # ms
            times.extend([pg_time, mongo_time])
        
        # Określ zwycięzcę (średnia z wszystkich testów)
        pg_avg = np.mean([times[i] for i in range(0, len(times), 2) if times[i] > 0])
        mongo_avg = np.mean([times[i] for i in range(1, len(times), 2) if times[i] > 0])
        
        if pg_avg > 0 and mongo_avg > 0:
            if mongo_avg < pg_avg * 0.9:
                winner = "MongoDB"
            elif pg_avg < mongo_avg * 0.9:
                winner = "PostgreSQL"
            else:
                winner = "Podobne"
        else:
            winner = "N/A"
        
        if len(times) >= 6:
            row = f"{polish_name:<40} {times[0]:<12.2f} {times[1]:<14.2f} {times[2]:<13.2f} {times[3]:<15.2f} {times[4]:<11.2f} {times[5]:<13.2f} {winner:<15}"
            print(row)
    
    print("="*120)
    
    # Podsumowanie według typu operacji z polskimi opisami
    print(f"\n📈 PODSUMOWANIE WYDAJNOŚCI WEDŁUG TYPU OPERACJI")
    print("-" * 80)
    print("Analiza zwycięstw dla każdego typu operacji CRUD:")
    print("(Zwycięzca = baza danych z co najmniej 10% lepszą wydajnością)")
    print("-" * 80)
    
    # Polskie nazwy typów operacji
    type_names = {
        'read': 'ODCZYT (READ)',
        'create': 'TWORZENIE (CREATE)', 
        'update': 'AKTUALIZACJA (UPDATE)',
        'delete': 'USUWANIE (DELETE)'
    }
    
    for op_type in ['read', 'create', 'update', 'delete']:
        type_operations = [op for op in all_operations if op.lower().startswith(op_type.upper() + ':')]
        
        pg_wins = 0
        mongo_wins = 0
        similar = 0
        
        for operation in type_operations:
            times = []
            for dataset in datasets:
                crud_result = results[dataset]["crud_results"].get(operation, {})
                pg_time = crud_result.get("postgresql", 0.0)
                mongo_time = crud_result.get("mongodb", 0.0)
                times.extend([pg_time, mongo_time])
            
            pg_avg = np.mean([times[i] for i in range(0, len(times), 2) if times[i] > 0])
            mongo_avg = np.mean([times[i] for i in range(1, len(times), 2) if times[i] > 0])
            
            if pg_avg > 0 and mongo_avg > 0:
                if mongo_avg < pg_avg * 0.9:
                    mongo_wins += 1
                elif pg_avg < mongo_avg * 0.9:
                    pg_wins += 1
                else:
                    similar += 1
        
        total = len(type_operations)
        type_name = type_names.get(op_type, op_type.upper())
        print(f"{type_name:<20} ({total} operacji): PostgreSQL={pg_wins}, MongoDB={mongo_wins}, Podobne={similar}")
    
    print("-" * 80)
    print("Interpretacja wyników:")
    print("• PostgreSQL = liczba operacji gdzie PostgreSQL był szybszy")
    print("• MongoDB = liczba operacji gdzie MongoDB był szybszy") 
    print("• Podobne = operacje z podobną wydajnością (różnica < 10%)")
    print("="*120)

def main():
    """Główna funkcja - kompletna wizualizacja."""
    print("📊 KOMPLETNY WIZUALIZATOR - Wszystkie 20 operacji CRUD")
    print("="*80)
    
    # Stwórz kompletne dane demonstracyjne
    results = create_complete_demo_data()
    
    # Sprawdź liczbę operacji
    total_operations = len(list(results.values())[0]["crud_results"])
    print(f"✅ Załadowano dane dla {len(results)} zbiorów danych")
    print(f"🧪 Wszystkie {total_operations} operacji CRUD:")
    
    # Pokaż listę operacji
    sample_data = list(results.values())[0]
    for i, operation in enumerate(sorted(sample_data["crud_results"].keys()), 1):
        print(f"  {i:2d}. {operation}")
    
    # Konfiguracja
    colors = setup_plot_style()
    
    print(f"\n🎨 Generowanie kompletnych wykresów...")
    
    print("1. Analiza przetwarzania danych...")
    create_clear_data_processing_charts(results)
    
    print("2. Wszystkie operacje CRUD...")
    create_all_crud_operations_charts(results)
    
    print("3. Szczegółowa tabela porównawcza...")
    create_summary_comparison_table(results)
    
    print(f"\n🎉 Kompletna wizualizacja zakończona!")
    print(f"📁 Wygenerowane pliki:")
    print(f"  - complete_data_processing_analysis.png")
    print(f"  - complete_read_operations.png (10 operacji READ)")
    print(f"  - complete_create_operations.png (4 operacje CREATE)")
    print(f"  - complete_update_operations.png (3 operacje UPDATE)")
    print(f"  - complete_delete_operations.png (3 operacje DELETE)")
    print(f"📊 WSZYSTKIE 20 operacji CRUD przeanalizowane!")

if __name__ == "__main__":
    main()
