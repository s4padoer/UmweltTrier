from sqlalchemy import create_engine
import time
import select
from typing import Callable
import os
import pandas as pd
from pathlib import Path
import threading
from sqlalchemy.pool import QueuePool


lock = threading.Lock()
DATABASE_READONLY_URL = os.environ.get("DATABASE_READONLY_URL")
if DATABASE_READONLY_URL is None:
    from dotenv import load_dotenv
    pfad = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(pfad)
    DATABASE_READONLY_URL = os.environ.get("DATABASE_READONLY_URL")

# Globale Engine mit Verbindungspooling
engine = create_engine(
    DATABASE_READONLY_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True
)

def make_query_df(query, params=None):
    try:
        if params is not None and type(params) is dict:
            result = pd.read_sql(query, engine, params=params)
        else:
            result = pd.read_sql(query, engine)
        return result
    except Exception as e:
        print(f"Fehler bei make_query_df: {e}")
        return pd.DataFrame()


def make_query(query):
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return result
    except Exception as e:
        print(f"Fehler bei make_query: {e}")
        return None


def listen_for_notifications(*methods: Callable[[], None]):
    try:
        with engine.connect() as connection:
            endless_observation(connection, methods)
    except Exception as e:
        print(f"Fehler bei listen_for_notifications: {e}")


def get_databaseurl():
    return DATABASE_READONLY_URL


def endless_observation(connection, methods):
    conn = connection.connection
    cursor = conn.cursor()
    cursor.execute("LISTEN update_data;")
    conn.commit()

    print("Waiting for notifications on channel 'update_data'")
        
    try:
        while True:
            if select.select([conn], [], [], 5) == ([], [], []):
                continue
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                print(f"Received notification: {notify.payload}")
                for method in methods:
                    method()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Listener stopped.")
    finally:
        cursor.execute("UNLISTEN update_data;")
        conn.commit()
