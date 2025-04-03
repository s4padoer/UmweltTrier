from load_data import make_query_df
from editing import format_date_german
from sqlalchemy import text
import pandas as pd

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Setzen der Locale auf Deutsch
#import locale
#locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')

def get_luftqualitaet_data():
    query = """
                 SELECT schadstoff.kuerzel AS schadstoff_kuerzel, schadstoff.name AS schadstoff_name, 
                 zeitintervall.kuerzel AS zeitintervall_kuerzel, zeitintervall.langname AS zeitintervall_name,
                 luftqualitaet.wert, luftqualitaet.zeitpunkt
                 FROM luftqualitaet
                 JOIN schadstoff ON luftqualitaet.schadstoff_ident = schadstoff.ident
                 JOIN zeitintervall ON luftqualitaet.zeitintervall_ident = zeitintervall.ident
                 JOIN einheit ON schadstoff.einheit_ident = einheit.ident
            """
    df = make_query_df(query)
    df.dropna(inplace = True)
    df_feinstaub = df[(df.schadstoff_kuerzel == 'PM10') & (df.zeitintervall_kuerzel == '1TMWGL')]
    df_ozon = df[(df.schadstoff_kuerzel == 'O3') & (df.zeitintervall_kuerzel == '8SMW')]
    df_kohlenmonoxid = df[(df.schadstoff_kuerzel == 'CO') & (df.zeitintervall_kuerzel == '8SMW')]
    df_feinstaub.sort_values("zeitpunkt", inplace=True)
    df_ozon.sort_values("zeitpunkt", inplace=True)
    df_kohlenmonoxid.sort_values("zeitpunkt", inplace=True)
    return df_feinstaub, df_ozon, df_kohlenmonoxid

def get_grenzwerte():
    query = """
                 SELECT schadstoff.kuerzel AS schadstoff_kuerzel, wert, anmerkung
                 FROM grenzwerte_luftschadstoffe
                 JOIN schadstoff ON schadstoff.ident = grenzwerte_luftschadstoffe.schadstoff_ident
            """
    df = make_query_df(query)
    return df


def get_verkehrsinfo():
    # Gleitender Durchschnitt, wie es bei den Luft-Daten der Fall ist 
    query = """
            SELECT freeflowtraveltime/currenttraveltime AS ratio_traveltime, 
            AVG(currentspeed/freeflowspeed ) OVER (
                ORDER BY zeitpunkt
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ratio_speed, wetterstation_ident, zeitpunkt
            FROM verkehr
            WHERE wetterstation_ident = 6
            """
    df = make_query_df(query)
    df.sort_values("zeitpunkt", inplace=True)
    return df


def get_alternative_luftqualitaet_plot():
    df_feinstaub, df_ozon, df_kohlenmonoxid = get_luftqualitaet_data()
    df_verkehr = get_verkehrsinfo()
    fig = get_base_plot(df_feinstaub, df_ozon, df_kohlenmonoxid)
    fig.add_trace( go.Scatter(x=df_verkehr['zeitpunkt'], y=df_verkehr['ratio_traveltime'], 
                   name='Verhältnis freie / aktuelle Reisezeit', line=dict(color='red'),
                   connectgaps=False,
                    hovertemplate='%{y:.1f}',
                   ),
        secondary_y=True)
    fig.update_layout(
    title_text="Luftqualität und Verkehrsdaten",
    xaxis_title="Zeit",
    yaxis_title="µg/m³",
    yaxis2_title="mg/m³",
    yaxis3=dict(
        title="Ratio freie vs. aktuelle Reisezeit / Geschwindigkeit",
        anchor="free",
        overlaying="y",
        side="right",
        position=1
        )
    )
    return fig


def get_base_plot(df_feinstaub, df_ozon, df_kohlenmonoxid):
    # Erstellen Sie eine Figure mit Subplots
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Feinstaub-Plot (linke y-Achse)
    fig.add_trace(
        go.Scatter(x=df_feinstaub['zeitpunkt'], y=df_feinstaub['wert'], 
                   name='Feinstaub', line=dict(color='blue'),
                   connectgaps=False,
                    hovertemplate='%{y:.1f} µg/m³'),
        secondary_y=False
    )

    # Ozon-Plot (linke y-Achse)
    fig.add_trace(
        go.Scatter(x=df_ozon['zeitpunkt'], y=df_ozon['wert'], 
                   name='Ozon', line=dict(color='green'),
                   connectgaps=False,
                    hovertemplate='%{y:.1f} µg/m³'),
        secondary_y=False
    )

    # Kohlenmonoxid-Plot (rechte y-Achse)
    fig.add_trace(
        go.Scatter(x=df_kohlenmonoxid['zeitpunkt'], y=df_kohlenmonoxid['wert'], 
                   name='Kohlenmonoxid', line=dict(color='orange'),
                   connectgaps=False,
                    hovertemplate='%{y:.1f} mg/m³',
                   ),
        secondary_y=True
    )

    # Layout anpassen
    fig.update_layout(
        title_text='Luftqualitätsdaten 2024',
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="x unified"
    )

    # Linke Y-Achse anpassen (Feinstaub und Ozon)
    fig.update_yaxes(
        title_text='Feinstaub und Ozon (µg/m³)',
        secondary_y=False,
    )

    # Rechte Y-Achse anpassen (Kohlenmonoxid)
    fig.update_yaxes(
        title_text='Kohlenmonoxid (mg/m³)',
        secondary_y=True,
    )

    fig.update_xaxes(tickformat='%d.%m.%y')
    # Update der y-Achsen
    fig.update_yaxes(title_text="µg/m³", secondary_y=False)
    fig.update_yaxes(title_text="mg/m³", secondary_y=True)
    return fig


def get_luftqualitaet_plot():
    
    df_feinstaub, df_ozon, df_kohlenmonoxid = get_luftqualitaet_data()
    grenzwerte = get_grenzwerte()

    fig = get_base_plot(df_feinstaub, df_ozon, df_kohlenmonoxid)
    # Grenzwertlinie hinzufügen
    limit =grenzwerte.loc[grenzwerte["schadstoff_kuerzel"]=="PM10","wert"].iloc[0]
    fig.add_trace(
        go.Scatter(
            x=[min(df_feinstaub["zeitpunkt"]), max(df_feinstaub["zeitpunkt"])],
            y=[limit, limit],
            mode='lines',
            line=dict(color="blue", dash='dash'),
            name="Feinstaub-Grenzwert",
            hovertemplate=f"Grenzwert: {limit} µg/m³",
            opacity=0.7
        ),
        secondary_y=False  # Dritte Linie auf sekundärer y-Achse
    )
    
    limit =grenzwerte.loc[grenzwerte["schadstoff_kuerzel"]=="O3","wert"].iloc[0]
    fig.add_trace(
        go.Scatter(
            x=[min(df_ozon["zeitpunkt"]), max(df_ozon["zeitpunkt"])],
            y=[limit, limit],
            mode='lines',
            line=dict(color="green", dash='dash'),
            name="Ozon-Grenzwert",
            hovertemplate=f"Grenzwert: {limit} µg/m³",
            opacity=0.7
        ),
        secondary_y=False  # Dritte Linie auf sekundärer y-Achse
    )
    
    limit =grenzwerte.loc[grenzwerte["schadstoff_kuerzel"]=="CO","wert"].iloc[0]
    fig.add_trace(
        go.Scatter(
            x=[min(df_kohlenmonoxid["zeitpunkt"]), max(df_kohlenmonoxid["zeitpunkt"])],
            y=[limit, limit],
            mode='lines',
            line=dict(color="orange", dash='dash'),
            name="Kohlenmonoxid-Grenzwert",
            hovertemplate=f"Grenzwert: {limit} µg/m³",
            opacity=0.7
        ),
        secondary_y=True  # Dritte Linie auf sekundärer y-Achse
    )
    
    fig.add_annotation(
        text="Datenquelle:\nUmweltbundesamt",
        xref="paper",
        yref="paper",
        x=0.95, y=-0.1, # x,y coordinates (0,0) is bottom left
        showarrow=False, # No arrow
        font=dict(size=12) # Font size
    )
    return fig

    