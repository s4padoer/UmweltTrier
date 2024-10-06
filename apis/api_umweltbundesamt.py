import requests
import os
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text
import json
import numpy as np
from load_data import get_engine

url = "https://www.umweltbundesamt.de/api/air_data/v2"

engine = get_engine()

def translate_zeitintervall_scope(x):
    if x == 1:
        return 6 # 1TMWGL
    elif x == 2:
        return 4 # 8SMW
    elif x == 3:
        return 2 # 1SMW
    elif x == 4:
        return 1 # 1TMW

def translate_scope_zeitintervall(x):
    if x == 6:
        return 1 # 1TMWGL
    elif x == 4:
        return 2 # 8SMW
    elif x == 2:
        return 3 # 1SMW
    elif x == 1:
        return 4 # 1TMW
    
# Finde je Wetterstation das letzte Datum heraus:

with engine.connect() as conn:
        query = text("""SELECT stationid, zeitintervall_ident, schadstoff_ident, MAX(zeitpunkt) 
                     FROM luftqualitaet 
                     JOIN wetterstation ON luftqualitaet.wetterstation_ident = wetterstation.ident
                     GROUP BY zeitintervall_ident, stationid, schadstoff_ident""")
        result = conn.execute(query)
        lastDate = result.fetchall()
        result = conn.execute(text("SELECT MAX(ident) FROM luftqualitaet"))
        lastIdent = result.fetchone()[0]
   
gestern = dt.datetime.now() - dt.timedelta(days=1)
    
headers = {
    "accept": "application/json"
}

def custom_datetime_parser(date_string):
    date_part, time_part = date_string.split()
    year, month, day = map(int, date_part.split('-'))
    hour, minute, second = map(int, time_part.split(':'))
        
    if hour == 24:
        hour = 0
        date_obj = dt.datetime(year, month, day, hour, minute, second) + dt.timedelta(days=1)
    else:
        date_obj = dt.datetime(year, month, day, hour, minute, second)
        
    return date_obj
    
summary_pd = pd.DataFrame(columns = ["wetterstation_ident", "schadstoff_ident", "zeitintervall_ident", "zeitpunkt", "produkt_ident", "wert"])
counter = lastIdent+1

for quadruple in lastDate:
    stationid = quadruple[0]
    scope = translate_zeitintervall_scope(quadruple[1])
    schadstoff = quadruple[2]
    datum = quadruple[3]
    if datum >= gestern:
        continue
    queryParams = {
    "date_from": datum.strftime("%Y-%m-%d"),
    "date_to": gestern.strftime("%Y-%m-%d"),
    "time_from": datum.hour + 1, # Passt, weil die API von 1-24h geht
    "time_to": "24",
    "station": stationid,
    "component" : schadstoff,
    "scope" : scope
    }
    queryResult = requests.get(url+"/measures/json", params=queryParams, headers=headers)
    resultJson = queryResult.json()["data"]

    for key, value in  resultJson[stationid].items():
         zeitpunkt = custom_datetime_parser(key)
         wert = value[2]
         single_pd = pd.DataFrame({
             "ident" : [counter],
             "wetterstation_ident" : [5 if stationid == '1457' else 4],
             "schadstoff_ident" :[schadstoff], 
             "zeitintervall_ident" : [quadruple[1]], 
             "zeitpunkt" : [zeitpunkt], 
             "produkt_ident" : [4], 
             "wert": [wert]
             })
         summary_pd = pd.concat([summary_pd, single_pd], ignore_index=True)
         counter=counter+1

summary_pd.dropna(inplace=True)
summary_pd.to_sql("luftqualitaet", engine, if_exists="append", index=False)