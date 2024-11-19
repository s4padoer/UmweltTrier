# API zu TomTom

import json
import requests
from sqlalchemy import text
import pandas as pd
import datetime as dt
import os

from load_data import get_engine

# Wetterstationen des Umweltbundesamtes laden:
query = text(""" SELECT wetterstation.* FROM wetterstation 
                 JOIN dienst ON wetterstation.dienst_ident = dienst.ident WHERE dienst.kurzname = 'tomtom'
            """)

engine = get_engine()
wetterstationen = pd.read_sql_query(query, engine)

query = text(""" SELECT * FROM einheit WHERE kuerzel IN ('s', 'km/h')
             """)
einheiten = pd.read_sql_query(query, engine)
produkt = pd.read_sql_query(text("SELECT * FROM produkt WHERE kurzname = 'Traffic API'"), engine)

pfad_tomtom = os.path.join(os.path.dirname(__file__), "tomtom.json")

with open(pfad_tomtom, "r") as configfile:
    tomtom_access = json.load(configfile)
    key = tomtom_access["key"]

baseURL = "api.tomtom.com/traffic/services"
versionNumber = 4
style = "relative0"
zoom = 20

all_results = None

for i,row in wetterstationen.iterrows():
    point = (row["geo_breite"], row["geo_laenge"]) 
    f = "json"

    # Request:
    address = f"https://{baseURL}/{versionNumber}/flowSegmentData/{style}/{zoom}/json?"
    address = address + "point=" + "{}%2C{}".format(point[0], point[1]) +"&unit=" + "KMPH" + "&key=" + key
    resp = requests.get(address)
    if resp.status_code == 200:
        result = pd.DataFrame(json.loads(resp.content)).T
    if all_results is None:
        all_results = result
    else:
        all_results = pd.concat([all_results, result] )
timestamp = dt.datetime.now()

def ordne_zu_wetterstation(results, wetterstationen):
    wetterstation_ostallee = wetterstationen["ident"][wetterstationen["stationid"] == "1457"]
    wetterstation_pfalzel = wetterstationen["ident"][wetterstationen["stationid"] == "1465"]
    results["wetterstation_ident"] = results["frc"].apply(lambda x: wetterstation_ostallee.iloc[0] if x == "FRC1" else wetterstation_pfalzel.iloc[0])
    return results
            
results = ordne_zu_wetterstation(all_results, wetterstationen) 
results = results.loc[:, ~results.columns.isin(["frc", "coordinates","@version"])]
results.loc[:,["zeitpunkt"]] = timestamp

results.loc[:,["speed_einheit_ident"]] = einheiten["ident"][einheiten["kuerzel"]== 'km/h'].iloc[0]
results.loc[:,["traveltime_einheit_ident"]] = einheiten["ident"][einheiten["kuerzel"]== 's'].iloc[0]
results.loc[:,["produkt_ident"]] = produkt.ident.iloc[0]
results.columns = results.columns.str.lower()

results.to_sql('verkehr', engine, if_exists='append', index=False)

