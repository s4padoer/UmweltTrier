import json
from sqlalchemy import create_engine
import time
import select
from typing import Callable, List
import os
import pandas as pd
import threading

from sshtunnel import SSHTunnelForwarder

lock = threading.Lock()
pfad = os.path.join(os.path.dirname(__file__), "datenbank.json")

with open(pfad, "r") as configfile:
    config = json.load(configfile)


use_ssh = 'ssh_host' in config and 'ssh_user' in config and 'ssh_private_key_path' in config


def make_query_df(query, params=None):
    databaseurl = get_databaseurl()
    if use_ssh:
        with lock:
            with SSHTunnelForwarder(
            (config['ssh_host'], 22),  # SSH Server und Port
            ssh_username=config['ssh_user'],
            ssh_private_key=config['ssh_private_key_path'],
            remote_bind_address=('localhost', config["port"]),  # Remote PostgreSQL Server
            local_bind_address=('localhost', config["port"])  # Lokaler Port
        ) as tunnel:
                engine = create_engine(databaseurl, pool_size=5)
                if params is not None and type(params) is dict:
                    result = pd.read_sql(query, engine, params=params)
                else:
                    result = pd.read_sql(query, engine)
                return result
    else:
        engine = create_engine(databaseurl)
        if params is not None and type(params) is dict:
            result = pd.read_sql(query, engine, params=params)
        else:
            result = pd.read_sql(query, engine)
        return result
    

def make_query(query):
    databaseurl = get_databaseurl()
    if use_ssh:
        with lock:
            with SSHTunnelForwarder(
            (config['ssh_host'], 22),  # SSH Server und Port
            ssh_username=config['ssh_user'],
            ssh_private_key=config['ssh_private_key_path'],
            remote_bind_address=('localhost', config["port"]),  # Remote PostgreSQL Server
            local_bind_address=('localhost', config["port"])  # Lokaler Port
        ) as tunnel:
                engine = create_engine(databaseurl,  pool_size=5) 
                with engine.connect() as conn:
                    result = conn.execute(query)
                    return result
    else:
        engine = create_engine(databaseurl) 
        with engine.connect() as conn:
            result = conn.execute(query)
            return result


def listen_for_notifications( *methods: Callable[[], None]):
    databaseurl = get_databaseurl()
    if use_ssh:
        with SSHTunnelForwarder(
            (config['ssh_host'], 22),  # SSH Server und Port
            ssh_username=config['ssh_user'],
            ssh_private_key=config['ssh_private_key_path'],
            remote_bind_address=('localhost', config["port"]),  # Remote PostgreSQL Server
            local_bind_address=('localhost', config["port"])  # Lokaler Port
        ) as tunnel:
            engine = create_engine(databaseurl) 
            with engine.connect() as connection:
                endless_observation(connection, methods)
    else:
        engine = create_engine(databaseurl) 
        with engine.connect() as connection:
            endless_observation(connection, methods)


def get_databaseurl():
    databaseurl = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
                user=config["user"],
                password=config["password"],
                host=config["host"],
                port=config["port"],
                dbname=config["dbname"]
            )
    return databaseurl


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
