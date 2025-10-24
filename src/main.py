from src.config import *
from src.db.postgres_manager import PostgresManager
from src.db.mongo_manager import MongoManager

def run():
    print("🔌 Łączenie z Postgres & Mongo...")
    pg = PostgresManager(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
    mg = MongoManager(MONGO_URI, MONGO_DB)

    print("🧱 Inicjalizacja schematu / indeksów...")
    pg.init_schema()
    mg.init_schema()

    print(f"📊 Przetwarzanie pliku CSV: {CSV_PATH}...")
    batch = []
    count = 0