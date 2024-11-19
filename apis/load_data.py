import json
import os
from sqlalchemy import create_engine

pfad = os.path.join(os.path.dirname(__file__), "datenbank.json")

with open(pfad, "r") as configfile:
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
