import json
import os
from sqlalchemy import create_engine

#pfad = os.path.join(os.path.dirname(__file__), "datenbank.json")
#
#with open(pfad, "r") as configfile:
#    config = json.load(configfile)

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL is None:
    from dotenv import load_dotenv
    pfad = os.path.join( os.path.dirname(__file__), "..", ".env")
    load_dotenv(pfad) 
    DATABASE_URL = os.environ.get("DATABASE_URL")

def get_engine():
    if DATABASE_URL is None:
        raise ValueError("DATABASE_URL environment variable is not set")
    engine = create_engine(DATABASE_URL)
    return engine
