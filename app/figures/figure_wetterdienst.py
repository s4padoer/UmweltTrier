from sqlalchemy import text
import pandas as pd
import plotly_express as px
from plotly import graph_objects as go
from plotly.subplots import make_subplots
import load_data


def get_referenzdata():
    referenz_query = "SELECT * FROM referenz_temperatur"
    referenz = load_data.make_query_df(referenz_query)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz = avg_referenz.reset_index()
    avg_referenz["zeitpunkt"] = pd.to_datetime(dict(year = 2024, month=avg_referenz["monat"], day = avg_referenz["tag"])) 
    
    referenz_query = "SELECT * FROM referenz_niederschlag"
    referenz = load_data.make_query_df(referenz_query)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz2 = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz2 = avg_referenz.reset_index()
    avg_referenz2["zeitpunkt"] = pd.to_datetime(dict(year = 2024, month=avg_referenz["monat"], day = avg_referenz["tag"])) 
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
        new_ref_temp.zeitpunkt = pd.to_datetime(dict(year = d.year, month= new_ref_temp.zeitpunkt.dt.month, day = new_ref_temp.zeitpunkt.dt.day))
        new_ref_percip.zeitpunkt = pd.to_datetime(dict(year = d.year, month= new_ref_percip.zeitpunkt.dt.month, day = new_ref_percip.zeitpunkt.dt.day))
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
    fig.update_layout(title_text="Tagestemperatur und Niederschlagsmenge 2024",
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
                   line=dict(color="red"), name="Durchschnittl. Tagestemperatur 2024",
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
                   line=dict(color="blue"), name="Tagesniederschlag 2024",
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
    temperatur_figure.update_traces(selector=dict(name = 'Durchschnittl. Tagestemperatur 2024'), x=avg_temp['zeitpunkt'], y=avg_temp['wert'])
    temperatur_figure.update_traces(selector=dict(name = "Tagesniederschlag 2024"), x=avg_percip['zeitpunkt'], y=avg_temp['wert'])

