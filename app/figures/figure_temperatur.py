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
    return avg_referenz


def get_referenzplot():
    referenzdata = get_referenzdata()
    currentdata = get_currentdata()
    fig = make_subplots()
    fig.add_trace(
        go.Scatter(
            x=referenzdata["zeitpunkt"], y = referenzdata["wert"],
            name="Durchschnittl. Tagestemperatur während Referenzzeitraums 1961-1990",
            line=dict(color="red"),
            hovertemplate='%{x}<br>%{y:.1f} °C<extra></extra>',
        )
    )
    fig.update_xaxes(tickformat="%d.%m.%y")
    fig.update_yaxes(title_text="°Celsius")
    fig.update_layout(title_text="Tagestemperatur 2024",
                    legend=dict(
                    orientation="h",  # Horizontale Ausrichtung
                    yanchor="bottom",
                    y=1.02,  # Position leicht über dem Plot
                    xanchor="center",
                    x=0.5  # Zentriert
                    )
    )
    
    fig.add_annotation(
        text="Datenquelle:\nDeutscher Wetterdienst",
        xref="paper",
        yref="paper",
        x=0.95, y=0.05, # x,y coordinates (0,0) is bottom left
        showarrow=False, # No arrow
        font=dict(size=12) # Font size
    )
    fig.add_trace(
        go.Scatter(x=currentdata["zeitpunkt"], y=currentdata["wert"],
                   line=dict(color="blue"), name="Durchschnittl. Tagestemperatur 2024",
                   hovertemplate="%{x}<br>%{y:.1f} °C<extra></extra>")
    )
    return fig
    

def get_currentdata():
    temperatur_query = text("SELECT * FROM temperatur")
    engine = load_data.get_engine()
    df = pd.read_sql_query(temperatur_query, engine)
    avg_temp = df.groupby(["zeitpunkt"])["wert"].mean().to_frame()
    avg_temp = avg_temp.reset_index()
    return avg_temp
    
    
def get_timeseries_temperatur():
    referenzplot = get_referenzplot()
    currentdata = get_currentdata()
    tempplot = px.line(currentdata, x='zeitpunkt', y='wert', title='Temperatur in Trier 2024')
    referenzplot.add_trace(go.Scatter(x=tempplot.data[0]['x'], y=tempplot.data[0]['y'], name='Messdaten 2024', mode='lines', line=dict(color='blue')))
    return referenzplot


def update_figure(temperatur_figure):
    avg_temp = get_currentdata()
    temperatur_figure.update_traces(selector=dict(name = 'Durchschnittl. Tagestemperatur 2024'), x=avg_temp['zeitpunkt'], y=avg_temp['wert'])

