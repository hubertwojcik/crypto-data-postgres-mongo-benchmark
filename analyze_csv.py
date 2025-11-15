#!/usr/bin/env python3
"""
Script to analyze and clean CSV data before loading into databases.

Usage:
    python analyze_csv.py

This script will:
1. Load and analyze the CSV file
2. Show data quality statistics
3. Clean the data
4. Validate data quality
5. Show a sample of cleaned data
"""

import sys
import os
import pandas as pd

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.config import CSV_PATH


def analyze_csv_data(csv_path: str):
    """Przeanalizuj dane CSV i wyświetl statystyki."""
    print(f"📊 Analiza danych z pliku: {csv_path}")
    
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


def show_sample_data(df, n=5):
    """Pokaż przykładowe dane."""
    print(f"\n📋 Przykładowe dane (pierwsze {n} rekordów):")
    print("-" * 80)
    
    for i, (_, row) in enumerate(df.head(n).iterrows()):
        print(f"\nRekord {i+1}:")
        print(f"  User: {row.get('user_name', 'N/A')}")
        print(f"  Text: {str(row.get('text', 'N/A'))[:100]}...")
        print(f"  Date: {row.get('date', 'N/A')}")
        print(f"  Hashtags: {row.get('hashtags', 'N/A')}")
        print(f"  Followers: {row.get('user_followers', 'N/A')}")


def main():
    """Main analysis function."""
    print("🚀 Analiza danych CSV...")
    
    try:
        # Sprawdź czy plik istnieje
        if not os.path.exists(CSV_PATH):
            print(f"❌ Plik CSV nie istnieje: {CSV_PATH}")
            return 1
        
        # Analizuj dane
        df_raw = analyze_csv_data(CSV_PATH)
        
        # Wyczyść dane
        df_cleaned = clean_csv_data(df_raw)
        
        # Waliduj jakość
        data_quality_ok = validate_data_quality(df_cleaned)
        
        # Pokaż przykładowe dane
        show_sample_data(df_cleaned)
        
        print(f"\n📊 Podsumowanie:")
        print(f"  Oryginalne dane: {len(df_raw):,} rekordów")
        print(f"  Po czyszczeniu: {len(df_cleaned):,} rekordów")
        print(f"  Jakość danych: {'✅ OK' if data_quality_ok else '⚠️ Problemy'}")
        
        if data_quality_ok:
            print(f"\n✅ Dane są gotowe do załadowania do baz danych!")
            print(f"Uruchom: python -m src.main")
        else:
            print(f"\n⚠️  Sprawdź problemy przed ładowaniem danych.")
        
        return 0
        
    except Exception as e:
        print(f"❌ Błąd podczas analizy: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())



