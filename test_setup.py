#!/usr/bin/env python3
"""
Test script to verify database connections and basic functionality.

Usage:
    python test_setup.py
"""

import sys
import os

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager


def test_connections():
    """Test database connections."""
    print("🔌 Testing database connections...")
    
    try:
        # Test PostgreSQL connection
        print("  Testing PostgreSQL...")
        pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
        with pg.conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            print(f"    ✅ PostgreSQL connected: {version[:50]}...")
        pg.close()
        
        # Test MongoDB connection
        print("  Testing MongoDB...")
        mg = MongoManager(MONGO_URI, MONGO_DB)
        # Test connection by pinging
        mg.client.admin.command('ping')
        print(f"    ✅ MongoDB connected")
        mg.close()
        
        print("✅ All database connections successful!")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_csv_loading():
    """Test CSV file loading."""
    print("\n📁 Testing CSV file...")
    
    try:
        from src.etl.load_tweets import load_csv
        
        # Check if file exists
        if not os.path.exists(CSV_PATH):
            print(f"❌ CSV file not found: {CSV_PATH}")
            return False
        
        # Try to load first few rows
        rows_iter = load_csv(CSV_PATH)
        first_row = next(rows_iter)
        
        print(f"    ✅ CSV file loaded successfully")
        print(f"    📊 Sample row keys: {list(first_row.keys())}")
        print(f"    📊 Sample hashtags: {first_row.get('hashtags', [])[:3]}")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV loading failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Testing project setup...")
    
    # Test database connections
    if not test_connections():
        print("\n❌ Database connection test failed!")
        print("Make sure to run: docker compose up -d")
        return 1
    
    # Test CSV loading
    if not test_csv_loading():
        print("\n❌ CSV loading test failed!")
        return 1
    
    print("\n✅ All tests passed! Project is ready to run.")
    print("\nNext step: python -m src.main")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())



