from typing import Any, List
import pandas as pd
import numpy as np
import re, ast
from datetime import timedelta

class DataPrecleaner:
    def __init__(self):
        pass


    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        missing_mask = df["user_name"].isna() | (df["user_name"].str.strip() == "")
        df.loc[missing_mask, "user_name"] = [
        f"unknown_user_{i}" for i in range(1, missing_mask.sum() + 1)]


    def __to_bool(value: Any) -> bool:
        if pd.isna(value):
            return False
        s = str(value).strip().lower()
        return s in ('true', '1', 'yes', 'y', 't')
    
    def __parse_hashtags(value: Any) -> List[str]:
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
    
    def __generate_mock_user_name(self):
        return f"user_{uuid.uuid4().hex[:8]}"