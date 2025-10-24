import os
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", os.getenv("PG_DB", "social"))
PG_USER = os.getenv("POSTGRES_USER", os.getenv("PG_USER", "user"))
PG_PASS = os.getenv("POSTGRES_PASSWORD", os.getenv("PG_PASS", "pass"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "social")

CSV_PATH  = os.getenv("CSV_PATH", "data/tweets.csv")