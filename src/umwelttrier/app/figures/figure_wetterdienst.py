from sqlalchemy import text
import pandas as pd
import plotly_express as px
from plotly import graph_objects as go
from plotly.subplots import make_subplots
import umwelttrier.app.load_data as load_data
from datetime import datetime

current_year = datetime.now().year


def get_referenzdata():
    referenz_query = "SELECT * FROM referenz_temperatur"
    referenz = load_data.make_query_df(referenz_query)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz = avg_referenz.reset_index()
    # Filter out invalid dates like Feb 29 in non-leap years
    avg_referenz = avg_referenz[~((avg_referenz.tag == 29) & (avg_referenz.monat == 2))]
    avg_referenz["zeitpunkt"] = pd.to_datetime(avg_referenz.assign(year=current_year).rename(columns={"monat": "month", "tag": "day"})[["year", "month", "day"]])
    
    referenz_query = "SELECT * FROM referenz_niederschlag"
    referenz = load_data.make_query_df(referenz_query)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz2 = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz2 = avg_referenz.reset_index()
    # Filter out invalid dates like Feb 29 in non-leap years
    avg_referenz2 = avg_referenz2[~((avg_referenz2.tag == 29) & (avg_referenz2.monat == 2))]
    avg_referenz2["zeitpunkt"] = pd.to_datetime(avg_referenz2.assign(year=current_year).rename(columns={"monat": "month", "tag": "day"})[["year", "month", "day"]])
    return avg_referenz, avg_referenz2


def get_timeseries_temperatur():
    referenzdata_temp, referenzdata_percip = get_referenzdata()
    currentdata_temp, currentdata_percip = get_currentdata()
    dates = currentdata_temp[["zeitpunkt"]].groupby(currentdata_temp.zeitpunkt.dt.year).max()
    for i,d in enumerate(dates.zeitpunkt[1:].tolist()):
        new_ref_temp = referenzdata_temp[((referenzdata_temp.zeitpunkt.dt.month == d.month) &  
                                         (referenzdata_temp.zeitpunkt.dt.day <= d.day )) |
                                         (referenzdata_temp.zeitpunkt.dt.month < d.month)]
        new_ref_percip = referenzdata_percip[((referenzdata_percip.zeitpunkt.dt.month == d.month) &  
                                         (referenzdata_percip.zeitpunkt.dt.day <= d.day )) |
                                         (referenzdata_percip.zeitpunkt.dt.month < d.month)]
        new_ref_temp = new_ref_temp[~((new_ref_temp.zeitpunkt.dt.day == 29)&(new_ref_temp.zeitpunkt.dt.month==2))]
        new_ref_percip = new_ref_percip[~((new_ref_percip.zeitpunkt.dt.day == 29)&(new_ref_percip.zeitpunkt.dt.month==2))]
        new_ref_temp.zeitpunkt = new_ref_temp.zeitpunkt.map(lambda x: x.replace(year=d.year))
        new_ref_percip.zeitpunkt = new_ref_percip.zeitpunkt.map(lambda x: x.replace(year=d.year))
        referenzdata_temp = pd.concat([referenzdata_temp, new_ref_temp])
        referenzdata_percip = pd.concat([referenzdata_percip, new_ref_percip])
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(tickformat="%d.%m.%y")
    fig.update_yaxes(
        title_text='Temperatur in °C',
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text='Niederschlagsmenge in mm',
        secondary_y=True,
    )
    fig.update_layout(title_text=f"Tagestemperatur und Niederschlagsmenge {current_year}",
                    legend=dict(
                    orientation="h",  # Horizontale Ausrichtung
                    yanchor="bottom",
                    y=1.02,  # Position leicht über dem Plot
                    xanchor="center",
                    x=0.5  # Zentriert
                    )
    )
    
    fig.add_trace(
        go.Scatter(
            x=referenzdata_temp["zeitpunkt"], y = referenzdata_temp["wert"],
            name="Durchschnittl. Tagestemperatur während Referenzzeitraum 1961-1990",
            line=dict(color="purple", dash="dash"),
            hovertemplate='%{x}<br>%{y:.1f} °C<extra></extra>',
        ),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=currentdata_temp["zeitpunkt"], y=currentdata_temp["wert"],
                   line=dict(color="red"), name=f"Durchschnittl. Tagestemperatur {current_year}",
                   hovertemplate="%{x}<br>%{y:.1f} °C<extra></extra>"),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=referenzdata_percip["zeitpunkt"], y = referenzdata_percip["wert"],
            name="Tagesniederschlag während Referenzzeitraum 1961-1990",
            line=dict(color="lightblue", dash="dash"),
            hovertemplate='%{x}<br>%{y:.1f} mm<extra></extra>',
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(x=currentdata_percip["zeitpunkt"], y=currentdata_percip["wert"],
                   line=dict(color="blue"), name=f"Tagesniederschlag {current_year}",
                   hovertemplate="%{x}<br>%{y:.1f} mm<extra></extra>"),
        secondary_y=True
    )
    fig.add_annotation(
        text="Datenquelle:\nDeutscher Wetterdienst",
        xref="paper",
        yref="paper",
        x=0.95, y=-0.1, # x,y coordinates (0,0) is bottom left
        showarrow=False, # No arrow
        font=dict(size=12) # Font size
    )
    return fig
    

def get_currentdata():
    temperatur_query = "SELECT * FROM temperatur"
    df = load_data.make_query_df(temperatur_query)
    avg_temp = df.groupby(["zeitpunkt"])["wert"].mean().to_frame()
    avg_temp = avg_temp.reset_index()
    percip_query = "SELECT * FROM niederschlag"
    df = load_data.make_query_df(percip_query)
    avg_percip = df.groupby(["zeitpunkt"])["wert"].mean().to_frame()
    avg_percip = avg_percip.reset_index()
    return avg_temp, avg_percip


def update_figure(temperatur_figure):
    avg_temp, avg_percip = get_currentdata()
    temperatur_figure.update_traces(selector=dict(name = f'Durchschnittl. Tagestemperatur {current_year}'), x=avg_temp['zeitpunkt'], y=avg_temp['wert'])
    temperatur_figure.update_traces(selector=dict(name = f"Tagesniederschlag {current_year}"), x=avg_percip['zeitpunkt'], y=avg_temp['wert'])

