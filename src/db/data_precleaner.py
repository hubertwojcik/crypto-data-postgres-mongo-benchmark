from typing import Any, List
import pandas as pd
import numpy as np
import re, ast
import time
from datetime import timedelta

class DataPrecleaner:
    def __init__(self):
        pass

    def analyze_data(self, df: pd.DataFrame) -> dict:
        """Przeanalizuj dane przed czyszczeniem."""
        print("📊 Analiza danych CSV...")
        
        analysis = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'missing_data': {},
            'data_types': {},
            'sample_values': {}
        }
        
        # Analiza brakujących danych
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / len(df)) * 100
            analysis['missing_data'][col] = {
                'count': missing_count,
                'percentage': missing_pct
            }
        
        # Typy danych
        analysis['data_types'] = df.dtypes.to_dict()
        
        # Przykładowe wartości (pierwsze 3 kolumny)
        for col in df.columns[:3]:
            sample_values = df[col].dropna().head(3).tolist()
            analysis['sample_values'][col] = sample_values
        
        print(f"  📈 Rekordów: {analysis['total_records']:,}")
        print(f"  📈 Kolumn: {analysis['total_columns']}")
        
        return analysis

    def clean_data_timed(self, df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
        """Wyczyść dane z pomiarem czasu."""
        print("🧹 Czyszczenie danych CSV...")
        start_time = time.perf_counter()
        
        cleaned_df = self.clean_data(df)
        
        elapsed = time.perf_counter() - start_time
        print(f"✅ Dane wyczyszczone w {elapsed:.4f}s")
        return cleaned_df, elapsed

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = [
            "user_name","user_location","user_description","user_created",
            "user_followers","user_friends","user_favourites","user_verified",
            "date","text","hashtags","source","is_retweet"
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.NA
        missing_mask = df["user_name"].isna() | (df["user_name"].astype(str).str.strip() == "")
        if missing_mask.any():
            df.loc[missing_mask, "user_name"] = [
                f"unknown_user_{i}" for i in range(1, missing_mask.sum() + 1)
            ]
        # alternatywnie (losowo): df.loc[missing_mask, "user_name"] = [self.__generate_mock_user_name() for _ in range(missing_mask.sum())]

        # 2) Teksty – uzupełnienia domyślne
        df["user_location"]    = df["user_location"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
        df["user_description"] = df["user_description"].fillna("No description").astype(str).str.strip().replace("", "No description")
        df["text"]             = df["text"].fillna("No content").astype(str)
        # Źródło (czasem bywa HTML – szybkie odszumienie)
        df["source"] = df["source"].fillna("Unknown source").astype(str)
        df["source"] = df["source"].str.replace(r"<.*?>", "", regex=True).str.strip().replace("", "Unknown source")

        # 3) Liczbowe → int (braki = 0)
        for col in ["user_followers", "user_friends", "user_favourites"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # 4) Bool → True/False
        df["user_verified"] = df["user_verified"].map(self.__to_bool)
        df["is_retweet"]    = df["is_retweet"].map(self.__to_bool)

        # 5) Daty
        df["user_created"] = pd.to_datetime(df["user_created"], errors="coerce")
        # mediana daty user_created dla braków (jeśli wszystkie NaT, użyj "now()")
        if df["user_created"].notna().any():
            median_uc = df["user_created"].dropna().median()
        else:
            median_uc = pd.Timestamp.utcnow()
        df["user_created"] = df["user_created"].fillna(median_uc)

        # data tweeta – kluczowa do porównań czasowych
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if df["date"].isna().any():
            # Użyj forward fill z bardziej efektywnym podejściem
            df["date"] = df["date"].fillna(method='ffill')
            # Jeśli nadal są NaT (pierwsze rekordy), wypełnij aktualną datą
            df["date"] = df["date"].fillna(pd.Timestamp.utcnow())

        # 6) Hashtagi → lista[str]
        df["hashtags"] = df["hashtags"].map(self.__parse_hashtags)
        # gwarancja typu listy
        df["hashtags"] = df["hashtags"].apply(lambda x: x if isinstance(x, list) else [])

        # 7) Finalne uzupełnienia, by nie zostały NaN/NaT
        df = df.fillna({
            "user_name": "unknown_user",
            "user_location": "unknown",
            "user_description": "No description",
            "source": "Unknown source",
        })

        # 8) (opcjonalnie) sanity-check typów pod DB
        #  - pandas Timestamp -> ok dla psycopg2/pymongo
        #  - list[str] w "hashtags" -> ok dla Mongo; do CSV można potem joinować
        return df

    def __to_bool(self, value: Any) -> bool:
        if pd.isna(value):
            return False
        s = str(value).strip().lower()
        return s in ('true', '1', 'yes', 'y', 't')
    
    def __parse_hashtags(self, value: Any) -> List[str]:
        if pd.isna(value) or value == "":
            return []
        try:
            tags = ast.literal_eval(str(value))
            if isinstance(tags, list):
                return [str(tag).strip().lstrip("#").lower() for tag in tags if str(tag).strip() ]
        except Exception:
            s = str(value).replace("#",' ')
            return [t.strip().lower() for t in re.split(r"[ ,]+", s) if t.strip()]
        return []
    
