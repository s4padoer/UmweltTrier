## API Call fuer taeglichen niederschlag
import datetime as dt
import pandas as pd
from wetterdienst.provider.dwd.observation import DwdObservationRequest, DwdObservationPeriod, DwdObservationResolution, DwdObservationParameter
from sqlalchemy import text
import numpy as np
import sys
from load_data import get_engine

engine = get_engine()

with engine.connect() as conn:
        query = text("SELECT MAX(zeitpunkt) FROM niederschlag")
        result = conn.execute(query)
        lastDate = result.fetchone()[0]
        query = text(""" SELECT wetterstation.* FROM wetterstation 
                     JOIN dienst ON wetterstation.dienst_ident = dienst.ident 
                     WHERE dienst.kurzname = 'dwd'
                     """)
        wetterstationen = pd.read_sql_query(query, engine)
        query = text(""" SELECT niederschlagsart.* FROM niederschlagsart 
                     JOIN produkt ON produkt.ident = niederschlagsart.produkt_ident
                     JOIN dienst ON produkt.dienst_ident = dienst.ident 
                     WHERE dienst.kurzname = 'dwd'
                     """)
        niederschlagsart = pd.read_sql_query(query, engine)
        produkt = pd.read_sql_query(text(""" SELECT produkt.*
                                    FROM produkt
                                    JOIN dienst ON produkt.dienst_ident = dienst.ident
                                    WHERE dienst.kurzname = 'dwd' AND produkt.kurzname = 'precipitation height'
                                    """), engine)
        
if produkt.shape[0] != 1:
    print("Kein oder zu viele Produkte gefunden!")
    sys.exit()

wetterstation_petrisberg = wetterstationen["ident"][wetterstationen["stationid"] == "5100"].to_numpy()[0]
wetterstation_zewen = wetterstationen["ident"][wetterstationen["stationid"] == "5099"].to_numpy()[0]
 
 
startDate = dt.datetime(2024,1,1,0,0,1) #lastDate + dt.timedelta(days=1)

gestern = dt.datetime.now() - dt.timedelta(days=1)

if startDate.date() >= gestern.date():
    sys.exit()

request = DwdObservationRequest(
    parameter=[DwdObservationParameter.DAILY.PRECIPITATION_HEIGHT, DwdObservationParameter.DAILY.PRECIPITATION_FORM],
    resolution=DwdObservationResolution.DAILY,
    period=DwdObservationPeriod.RECENT,
    start_date=startDate,
    end_date=gestern
)

stations = request.filter_by_station_id(station_id=("5100", "5099", ))

def map_niederschlagsart(code, niederschlagsart):
    array = niederschlagsart.ident[niederschlagsart.code == code].to_numpy()
    if(len(array)) != 1:
        return None
    else:
        return array[0] 

for result in stations.values.query():    
    df = result.df
    df = df.drop_nulls()
    if (df is None) or (df.shape[0] == 0):
        continue
    df_niederschlag = df.filter( parameter="precipitation_height")
    df_niederschlag = df_niederschlag.with_columns(niederschlagsart_code = df.filter( parameter="precipitation_form")["value"].to_numpy().astype(int),
                                                   wert = df_niederschlag["value"],
                                                   zeitpunkt = df_niederschlag["date"])
    df_niederschlag = df_niederschlag.drop(["dataset", "value", "quality", "date", "parameter"])
    df_niederschlag = df_niederschlag.with_columns(
        wetterstation_ident=df_niederschlag["station_id"].map_elements(lambda x: wetterstation_petrisberg if x =='05100' else wetterstation_zewen, return_dtype=int),
        produkt_ident = produkt.ident.iloc[0],
        niederschlagsart_ident = df_niederschlag["niederschlagsart_code"].map_elements(lambda x: map_niederschlagsart(x, niederschlagsart), return_dtype=int))
    
    df_niederschlag = df_niederschlag.drop(["station_id", "niederschlagsart_code"])
    df = df_niederschlag.to_pandas()
    df.to_sql("niederschlag", con=engine, if_exists="append", index=False)

