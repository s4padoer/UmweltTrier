## API Call fuer taegliche durchschnittstemperatur
import datetime as dt
import pandas as pd
from wetterdienst.provider.dwd.observation import DwdObservationRequest, DwdObservationPeriod, DwdObservationResolution, DwdObservationParameter
from sqlalchemy import text
import numpy as np
import sys
from load_data import get_engine

engine = get_engine()

with engine.connect() as conn:
        query = text("SELECT MAX(zeitpunkt) FROM temperatur")
        result = conn.execute(query)
        lastDate = result.fetchone()[0]

startDate = lastDate + dt.timedelta(days=1)

gestern = dt.datetime.now() - dt.timedelta(days=1)

if startDate.date() >= gestern.date():
    sys.exit()

request = DwdObservationRequest(
    parameter=DwdObservationParameter.DAILY.TEMPERATURE_AIR_MEAN_200,
    resolution=DwdObservationResolution.DAILY,
    period=DwdObservationPeriod.RECENT,
    start_date=startDate,
    end_date=gestern
)

stations = request.filter_by_station_id(station_id=("5100", "5099", ))

def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius

for result in stations.values.query():    
    df = result.df
    if (df is None) or (df.shape[0] == 0):
        continue
    df = df.with_columns(
        value=df["value"].map_elements(lambda x: kelvin_to_celsius(float(x)), return_dtype=float),
        wetterstation_ident=df["station_id"].map_elements(lambda x: 1 if x =='05100' else 2, return_dtype=int),
        produkt_ident = 2)
    df = df.drop(["dataset", "parameter", "quality"])
    df = df.with_columns(
        wert = df["value"],
        zeitpunkt = df["date"])
    df = df.drop(["date", "station_id", "value"])
    # Es gibt wohl ein Problem mit den pandas versionen und to_sql
    with engine.connect() as conn:
        query = text("SELECT MAX(ident) FROM temperatur")
        result = conn.execute(query)
        max_ident = result.scalar()
        
    pandasDF = pd.DataFrame({ "ident" : np.arange(max_ident+1, max_ident + df.shape[0]+1),
                             "produkt_ident" : df["produkt_ident"],
                             "wetterstation_ident" : df["wetterstation_ident"],
                             "wert" : df["wert"],
                             "zeitpunkt" : df["zeitpunkt"]})
    pandasDF.dropna(inplace=True)
    pandasDF.to_sql('temperatur', engine, if_exists='append', index=False)

    








