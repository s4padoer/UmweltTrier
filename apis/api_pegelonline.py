import requests
import os
import pandas as pd
import datetime as dt
from sqlalchemy import text
from load_data import get_engine

url = "https://www.pegelonline.wsv.de/webservices/files/Wassertemperatur+Rohdaten/MOSEL/abd34ee6-a578-4639-b73d-fa4e08f40345"

csvFilename = "down.csv"

engine = get_engine()

with engine.connect() as conn:
        query = text("SELECT MAX(zeitpunkt) FROM wassertemperatur_mosel")
        result = conn.execute(query)
        lastDate = result.fetchone()[0]
        result = conn.execute(text("SELECT MAX(ident) FROM wassertemperatur_mosel"))
        lastIdent = result.fetchone()[0]

datum = lastDate.date()
gestern = (dt.datetime.now() - dt.timedelta(days=1)).date()

while datum < gestern:
    datum = datum + dt.timedelta(days=1)
    datumString = datum.strftime("%d.%m.%Y")
    completeUrl = url + "/" + datumString + "/" + csvFilename
    response = requests.get(completeUrl, allow_redirects=True)
    
    if response.status_code == 200:
        filename = f"apis/downloads/mosel_water_temperature/mosel_water_temperature_{datum.strftime('%Y-%m-%d')}.csv"
        with open(filename, 'wb') as file:
            file.write(response.content)
            print(f"File '{filename}' has been downloaded successfully.")
    else:
        print(f"Unexpected status code: {response.status_code}")
        
datum = lastDate.date()
dfDatabase = pd.DataFrame(columns = ["ident", "produkt_ident", "wetterstation_ident", "zeitpunkt", "wert"])
counter = 1

while datum < gestern:
    datum = datum + dt.timedelta(days=1)
    datumString = datum.strftime('%Y-%m-%d')
    filename = f"downloads/mosel_water_temperature/mosel_water_temperature_{datumString}.csv"
    try:
        df = pd.read_csv(filename, sep=";", decimal=",")
        wert = df.iloc[:,1].mean()
        dfInsert = pd.DataFrame({"ident" : [lastIdent + counter], "produkt_ident" : [3], "wetterstation_ident" : [3], "zeitpunkt" : [datum], "wert" : [wert]})
        dfDatabase = pd.concat([dfDatabase, dfInsert], ignore_index=True)
        os.remove(filename)
        counter+=1
    except:
         continue
    
dfDatabase.dropna(inplace=True)

if dfDatabase is not None and dfDatabase.shape[0] > 0:
    dfDatabase.to_sql('wassertemperatur_mosel', engine, if_exists='append', index=False)
    
    
