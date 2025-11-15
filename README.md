# 🚀 Crypto Data PostgreSQL vs MongoDB Benchmark

Projekt porównujący wydajność PostgreSQL i MongoDB na danych tweetów o Bitcoin (~22M rekordów).

## 📋 Funkcje

- **ETL Pipeline**: Ładowanie i normalizacja danych z CSV
- **Dual Database**: PostgreSQL (relacyjny) vs MongoDB (dokumentowy)
- **Basic Benchmarking**: Podstawowe testy wydajności po załadowaniu danych
- **Data Cleaning**: Automatyczne czyszczenie baz przed ładowaniem

## 🚀 Szybki start

### 1. Uruchom bazy danych
```bash
docker compose up -d
```

### 2. Zainstaluj zależności
```bash
python -m venv .venv && source .venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### 3. (Opcjonalnie) Sprawdź połączenia
```bash
python test_setup.py
```

### 4. (Opcjonalnie) Przeanalizuj dane CSV
```bash
python analyze_csv.py
```

### 5. Załaduj dane i uruchom benchmarki
```bash
python -m src.main
```

### 6. (Opcjonalnie) Interaktywne testy w Jupyter

**Opcja A: Jupyter przez Docker (zalecane)**
```bash
# Uruchom wszystkie serwisy
docker compose up -d

# Jeśli kontener Jupyter już był uruchomiony PRZED dodaniem zmiennych środowiskowych,
# musisz go przebudować (nie wystarczy restart):
docker compose stop jupyter
docker compose rm -f jupyter
docker compose up -d jupyter

# Lub użyj skryptu pomocniczego:
./restart_jupyter.sh

# Jupyter Lab będzie dostępny na http://localhost:8888
# Otwórz benchmark_analysis.ipynb w przeglądarce
```

**Uwaga:** 
- W kontenerze Docker automatycznie używane są nazwy serwisów (`postgres`, `mongo`) zamiast `localhost`
- Jeśli widzisz błąd połączenia, sprawdź komórkę "1.5. Sprawdzenie konfiguracji" w notebooku
- Jeśli zmienne środowiskowe pokazują `localhost` w Dockerze, przebuduj kontener

**Opcja B: Jupyter lokalnie**
```bash
jupyter lab benchmark_analysis.ipynb
# lub
jupyter notebook benchmark_analysis.ipynb
```

To polecenie:
- Przeanalizuje i wyczyści dane z CSV
- Wyczyści obie bazy danych
- Załaduje wyczyszczone dane do obu baz
- Uruchomi podstawowe testy wydajności

**Jupyter Notebook** pozwala na:
- Interaktywne testowanie funkcji z klas
- Wizualizację wyników benchmarków
- Eksperymentowanie z różnymi zapytaniami
- Analizę danych krok po kroku

## 📊 Struktura danych

### PostgreSQL (Model relacyjny)
- `users` - informacje o użytkownikach
- `tweets` - tweety z referencjami do użytkowników
- `hashtags` - hashtagi
- `sources` - źródła tweetów
- `tweet_hashtags` - relacja many-to-many

### MongoDB (Model dokumentowy)
- `tweets` - pojedyncza kolekcja z zagnieżdżonymi dokumentami użytkowników

## 🧹 Czyszczenie danych

Projekt automatycznie czyści dane CSV przed ładowaniem:

### Analiza danych
- Sprawdza brakujące wartości w każdej kolumnie
- Liczy unikalne wartości
- Pokazuje przykładowe dane
- Wyświetla statystyki jakości

### Czyszczenie danych
- **Usuwa duplikaty** - eliminuje powtarzające się rekordy
- **Czyści kolumny tekstowe** - usuwa puste stringi
- **Waliduje kluczowe pola** - `user_name`, `date`, `text` muszą być wypełnione
- **Naprawia wartości numeryczne** - zastępuje ujemne wartości i NaN zerami
- **Normalizuje boolean** - zastępuje NaN wartościami `False`
- **Uzupełnia źródła** - zastępuje puste źródła wartością "Unknown"

### Walidacja jakości
- Sprawdza unikalność `user_name`
- Weryfikuje poprawność dat
- Kontroluje wartości numeryczne
- Raportuje znalezione problemy

## 🔍 Podstawowe testy wydajności

Po załadowaniu danych automatycznie uruchamiane są:

### Test 1: Liczenie rekordów
- Porównanie szybkości zliczania tweetów i użytkowników
- PostgreSQL: `COUNT(*)` na tabelach
- MongoDB: `count_documents()` i `distinct()`

### Test 2: Najnowsze tweety
- Pobieranie 100 najnowszych tweetów z informacjami o użytkownikach
- PostgreSQL: `JOIN` z `ORDER BY` i `LIMIT`
- MongoDB: `find()` z `sort()` i `limit()`

### Test 3: Wyszukiwanie po hashtagach
- Filtrowanie tweetów zawierających hashtag "bitcoin"
- PostgreSQL: `JOIN` przez tabele `tweet_hashtags` i `hashtags`
- MongoDB: `find()` z filtrem na tablicy `hashtags`

## 🛠️ Konfiguracja

Zmienne środowiskowe (`.env`):
```bash
# PostgreSQL
POSTGRES_USER=user
POSTGRES_PASSWORD=pass
POSTGRES_DB=social

# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=social

# ETL
CSV_PATH=data/Bitcoin_tweets.csv
BATCH_SIZE=1000
```

## 📁 Struktura projektu

```
├── src/
│   ├── config.py          # Konfiguracja
│   ├── main.py            # ETL pipeline + podstawowe benchmarki
│   ├── db/
│   │   ├── postgres_manager.py
│   │   └── mongo_manager.py
│   └── etl/
│       └── load_tweets.py
├── data/
│   └── Bitcoin_tweets.csv
├── docker-compose.yaml
└── requirements.txt
```

## 🎯 Cel projektu

Porównanie wydajności dwóch różnych paradygmatów baz danych:
- **PostgreSQL**: Relacyjny model z normalizacją, JOIN-y, ACID
- **MongoDB**: Dokumentowy model, zagnieżdżone struktury, elastyczność

Na zbiorze danych tweetów o Bitcoin, testując różne scenariusze użycia i wzorce dostępu do danych.