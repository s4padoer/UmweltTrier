from sqlalchemy import text
import pandas as pd
import plotly_express as px
from plotly import graph_objects as go
from plotly.subplots import make_subplots
import load_data


def get_referenzdata():
    engine = load_data.get_engine()
    referenz_query = text("SELECT * FROM referenz_temperatur")
    referenz = pd.read_sql_query(referenz_query, engine)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz = avg_referenz.reset_index()
    avg_referenz["zeitpunkt"] = pd.to_datetime(dict(year = 2024, month=avg_referenz["monat"], day = avg_referenz["tag"])) 
    
    referenz_query = text("SELECT * FROM referenz_niederschlag")
    referenz = pd.read_sql_query(referenz_query, engine)
    # Zwei Wetterstationen j- also immer zwei werte...
    avg_referenz2 = referenz.groupby(["monat", "tag"])["wert"].mean().to_frame()
    avg_referenz2 = avg_referenz.reset_index()
    avg_referenz2["zeitpunkt"] = pd.to_datetime(dict(year = 2024, month=avg_referenz["monat"], day = avg_referenz["tag"])) 
    return avg_referenz, avg_referenz2


def get_timeseries_temperatur():
    referenzdata_temp, referenzdata_percip = get_referenzdata()
    currentdata_temp, currentdata_percip = get_currentdata()
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
    temperatur_query = text("SELECT * FROM temperatur")
    engine = load_data.get_engine()
    df = pd.read_sql_query(temperatur_query, engine)
    avg_temp = df.groupby(["zeitpunkt"])["wert"].mean().to_frame()
    avg_temp = avg_temp.reset_index()
    percip_query = text("SELECT * FROM niederschlag")
    df = pd.read_sql_query(percip_query, engine)
    avg_percip = df.groupby(["zeitpunkt"])["wert"].mean().to_frame()
    avg_percip = avg_percip.reset_index()
    return avg_temp, avg_percip


def update_figure(temperatur_figure):
    avg_temp, avg_percip = get_currentdata()
    temperatur_figure.update_traces(selector=dict(name = 'Durchschnittl. Tagestemperatur 2024'), x=avg_temp['zeitpunkt'], y=avg_temp['wert'])
    temperatur_figure.update_traces(selector=dict(name = "Tagesniederschlag 2024"), x=avg_percip['zeitpunkt'], y=avg_temp['wert'])

