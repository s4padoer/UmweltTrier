import json
from sqlalchemy import create_engine

with open("apis/datenbank.json", "r") as configfile:
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
