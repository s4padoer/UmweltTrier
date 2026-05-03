import json
from sqlalchemy import create_engine
import time
import select
from typing import Callable, List
import os
import pandas as pd
import threading


lock = threading.Lock()
DATABASE_READONLY_URL = os.environ.get("DATABASE_READONLY_URL")
if DATABASE_READONLY_URL is None:
    from dotenv import load_dotenv
    pfad = os.path.join( os.path.dirname(__file__), "..", ".env")
    load_dotenv(pfad)
    DATABASE_READONLY_URL = os.environ.get("DATABASE_READONLY_URL")


def make_query_df(query, params=None):
    databaseurl = get_databaseurl()
    engine = create_engine(databaseurl)
    if params is not None and type(params) is dict:
        result = pd.read_sql(query, engine, params=params)
    else:
        result = pd.read_sql(query, engine)
    return result
        
    

def make_query(query):
    databaseurl = get_databaseurl()
    engine = create_engine(databaseurl) 
    with engine.connect() as conn:
        result = conn.execute(query)
        return result
        

def listen_for_notifications( *methods: Callable[[], None]):
    databaseurl = get_databaseurl()
    engine = create_engine(databaseurl) 
    with engine.connect() as connection:
        endless_observation(connection, methods)
        

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
                # Hier aktualisieren wir die Grafiken und Daten
                for method in methods:
                    method()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Listener stopped.")
    finally:
        cursor.execute("UNLISTEN update_data;")
        conn.commit()
