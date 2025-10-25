"""
ETL module for loading and normalizing Twitter data from CSV files.

This module provides functionality to load Twitter data from CSV files,
normalize the data types, and prepare it for database insertion.
It handles various data type conversions including boolean parsing,
list parsing, datetime conversion, and numeric type casting.
"""

import ast 
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Generator


def _parse_bool(x: Any) -> Optional[bool]:
    """
    Parse a value to boolean, handling various string representations.
    
    Args:
        x: Value to parse (can be bool, string, or NaN)
        
    Returns:
        Boolean value if successfully parsed, None if input is NaN
        
    Examples:
        >>> _parse_bool("true")
        True
        >>> _parse_bool("1")
        True
        >>> _parse_bool("false")
        False
        >>> _parse_bool(None)
        None
    """
    # Handle NaN values
    if pd.isna(x): 
        return None
    
    # Return as-is if already boolean
    if isinstance(x, bool): 
        return x
    
    # Convert to string and normalize for comparison
    s = str(x).strip().lower()
    return s in ('true', '1', 'yes', 'y', 't')


def _parse_list(x: Any) -> List[str]:
    """
    Parse a value to a list of strings, handling various input formats.
    
    Attempts to parse the input as a Python literal list first,
    then falls back to comma-separated string parsing.
    
    Args:
        x: Value to parse (can be list, string, or NaN)
        
    Returns:
        List of strings, empty list if input is NaN or None
        
    Examples:
        >>> _parse_list("['tag1', 'tag2']")
        ['tag1', 'tag2']
        >>> _parse_list("tag1, tag2, tag3")
        ['tag1', 'tag2', 'tag3']
        >>> _parse_list(None)
        []
    """
    # Handle NaN and None values
    if pd.isna(x) or x is None: 
        return []
    
    # Return as-is if already a list
    if isinstance(x, list):
        return x
    
    s = str(x)
    
    # Try to parse as Python literal (e.g., "['a', 'b']")
    try:
        lst = ast.literal_eval(s)
        if isinstance(lst, list):
            # Clean hashtags by removing '#' prefix and whitespace
            return [str(t).strip().lstrip('#') for t in lst if str(t).strip()]
    except (ValueError, SyntaxError):
        # If literal_eval fails, fall back to string parsing
        pass
    
    # Parse comma-separated string
    # Remove brackets if present and split by comma
    s = s.strip().strip('[]')
    tokens = [t.strip().lstrip("#").strip("'\"") for t in s.split(",") if t.strip()]
    return tokens


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single row of Twitter data to proper data types.
    
    Converts numeric fields to integers, parses boolean fields,
    converts date fields to datetime objects, and processes hashtags.
    
    Args:
        row: Dictionary representing a single tweet record
        
    Returns:
        Dictionary with normalized data types
        
    Note:
        This function modifies the input dictionary in-place and also returns it.
    """
    # Convert numeric fields to integers, handling NaN values
    row["user_followers"] = None if pd.isna(row.get("user_followers")) else int(float(row["user_followers"]))
    row["user_friends"] = None if pd.isna(row.get("user_friends")) else int(row["user_friends"])
    row["user_favourites"] = None if pd.isna(row.get("user_favourites")) else int(row["user_favourites"])
    
    # Parse boolean field
    row["user_verified"] = _parse_bool(row.get("user_verified"))

    # Convert date fields to datetime objects
    for col in ("user_created", "date"):
        v = row.get(col)
        if pd.isna(v):
            row[col] = None
        else:
            # Use pandas to_datetime with error coercion for robust parsing
            row[col] = pd.to_datetime(v, errors="coerce", utc=False)
    
    # Parse hashtags list
    row["hashtags"] = _parse_list(row.get("hashtags"))
    
    return row


def load_csv(path: str) -> Generator[Dict[str, Any], None, None]:
    """
    Load and normalize Twitter data from a CSV file.
    
    Reads a CSV file, ensures all expected columns are present,
    fills missing values with defaults, and normalizes each row.
    
    Args:
        path: Path to the CSV file to load
        
    Returns:
        Generator yielding normalized tweet records as dictionaries
        
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        pd.errors.EmptyDataError: If the CSV file is empty
        
    Example:
        >>> for tweet in load_csv("tweets.csv"):
        ...     print(tweet["text"])
    """
    # Load CSV file into pandas DataFrame
    df = pd.read_csv(path)
    
    # Define expected columns for Twitter data
    expected_columns = [
        "user_name", "user_location", "user_description", "user_created",
        "user_followers", "user_friends", "user_favourites", "user_verified",
        "date", "text", "hashtags", "source", "is_retweet"
    ]
    
    # Add missing columns with None values
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    # Fill specific columns with default values for missing data
    df = df.fillna({"user_verified": False, "is_retweet": False})
    
    # Convert DataFrame to records and normalize each row
    # Using generator for memory efficiency with large datasets
    rows = (normalize_row(rec) for rec in df.to_dict("records"))
    return rows
