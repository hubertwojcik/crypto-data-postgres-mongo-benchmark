# Struktura Baz Danych

## PostgreSQL (Baza Relacyjna)

### Schemat Normalizowany
Baza wykorzystuje klasyczny model relacyjny z normalizacją danych do 3NF, rozdzielając dane na logiczne encje.

#### Tabele:

1. **`users`**
   - `id` (SERIAL PRIMARY KEY)
   - `user_name` (TEXT UNIQUE) - unikalna nazwa użytkownika
   - `user_location` (TEXT)
   - `user_description` (TEXT)
   - `user_created` (TIMESTAMP)
   - `user_followers` (BIGINT)
   - `user_friends` (BIGINT)
   - `user_favourites` (BIGINT)
   - `user_verified` (BOOLEAN)

2. **`sources`**
   - `id` (SERIAL PRIMARY KEY)
   - `name` (TEXT UNIQUE) - źródło tweeta (np. "Twitter Web App")

3. **`tweets`**
   - `id` (BIGSERIAL PRIMARY KEY)
   - `user_id` (INT) → FK do `users(id)`
   - `date` (TIMESTAMP)
   - `text` (TEXT)
   - `source_id` (INT) → FK do `sources(id)`
   - `is_retweet` (BOOLEAN)

4. **`hashtags`**
   - `id` (SERIAL PRIMARY KEY)
   - `tag` (TEXT UNIQUE) - unikalny hashtag

5. **`tweet_hashtags`** (tabela łącząca)
   - `tweet_id` (BIGINT) → FK do `tweets(id)` ON DELETE CASCADE
   - `hashtag_id` (INT) → FK do `hashtags(id)` ON DELETE CASCADE
   - PRIMARY KEY (tweet_id, hashtag_id)

### Indeksy:
- `idx_tweets_user_date` na `tweets(user_id, date)` - optymalizacja zapytań po użytkowniku i dacie
- `idx_hashtag_tag` na `hashtags(tag)` - szybkie wyszukiwanie po hashtagach

### Charakterystyka:
- ✅ Normalizacja danych (eliminacja redundancji)
- ✅ Relacje przez klucze obce z obsługą CASCADE
- ✅ Unikalność na poziomie bazy (user_name, tag, source)
- ✅ Optymalizacja przez indeksy złożone

---

## MongoDB (Baza Nierelacyjna)

### Schemat Denormalizowany
Baza wykorzystuje model dokumentowy z denormalizacją danych - wszystkie informacje o tweecie są przechowywane w jednym dokumencie.

#### Kolekcja: `tweets`

**Struktura dokumentu:**
```json
{
  "_id": ObjectId,
  "user": {
    "user_name": String,
    "user_location": String,
    "user_description": String,
    "user_created": ISODate,
    "user_followers": Number,
    "user_friends": Number,
    "user_favourites": Number,
    "user_verified": Boolean
  },
  "date": ISODate,
  "text": String,
  "hashtags": [String],  // tablica hashtagów
  "source": String,
  "is_retweet": Boolean
}
```

### Indeksy:
- `date` (ASCENDING) - sortowanie po dacie
- `user.user_name` (ASCENDING) - wyszukiwanie po użytkowniku
- `hashtags` (ASCENDING) - wyszukiwanie po hashtagach
- `is_retweet` (ASCENDING) - filtrowanie retweetów

### Charakterystyka:
- ✅ Denormalizacja (wszystkie dane w jednym dokumencie)
- ✅ Zagnieżdżone obiekty (dane użytkownika w `user`)
- ✅ Tablice (hashtagi jako lista w dokumencie)
- ✅ Brak relacji - dane są samowystarczalne
- ✅ Szybsze odczyty (jeden dokument = jeden tweet z pełnymi danymi)

---

## Porównanie Podejść

| Aspekt | PostgreSQL | MongoDB |
|--------|-----------|---------|
| **Model** | Relacyjny (normalizowany) | Dokumentowy (denormalizowany) |
| **Struktura** | 5 tabel + relacje | 1 kolekcja |
| **Dane użytkownika** | Osobna tabela + JOIN | Zagnieżdżone w dokumencie |
| **Hashtagi** | Tabela + tabela łącząca | Tablica w dokumencie |
| **Zapytania** | Wymagają JOIN-ów | Pojedyncze zapytanie |
| **Redundancja** | Minimalna | Wysoka (dane użytkownika powielone) |
| **Aktualizacja danych** | Łatwa (jedna tabela) | Trudniejsza (wiele dokumentów) |
| **Czytanie** | Wymaga JOIN-ów | Szybkie (jeden dokument) |

