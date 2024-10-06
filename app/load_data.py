import json
from sqlalchemy import create_engine
import time
import select
from typing import Callable, List


with open("app/datenbank.json", "r") as configfile:
    config = json.load(configfile)


def get_engine():
    databaseurl = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"]
    )
    engine = create_engine(databaseurl)
    return engine


def listen_for_notifications( *methods: Callable[[], None]):
    engine = get_engine()
    with engine.connect() as connection:
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