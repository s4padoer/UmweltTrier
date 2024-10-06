from load_data import get_engine
from sqlalchemy import text
import pandas as pd
import plotly.graph_objects as go

def get_ndvi_hist(lat, lon, fig):
    engine = get_engine()
    query = text("""
        WITH point AS (
            SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) AS geom
        )
        SELECT 
            ST_Value(rast, 1, point.geom)/32767.5-1 AS ndvi_value,
            jahr,
            monat,
            ident
        FROM ndvi, point
        WHERE ST_Intersects(rast, point.geom)
        ORDER BY jahr, monat
        """)
    ndvi = pd.read_sql(query, engine, params={'lon': lon, 'lat': lat})
    ndvi_avg = ndvi.groupby(["monat"])["ndvi_value"].mean().reset_index()
    month_names = ['Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez']
    ndvi_avg['monat_name'] = ndvi_avg['monat'].apply(lambda x: month_names[x-1])
    fig.update_traces(selector=dict(name = 'NDVI je Monat'), x=ndvi_avg['monat_name'], y=ndvi_avg['ndvi_value'])
    return fig
        
        
def create_empty_ndvi_figure():
    month_names = ['Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez']
    
    fig = go.Figure()
    
    # Fügen Sie eine leere Trace hinzu
    fig.add_trace(go.Bar(x=month_names, y=[0]*12, name='NDVI je Monat'))
    
    # Konfigurieren Sie das Layout
    fig.update_layout(
        xaxis = dict(
            tickmode = 'array',
            tickvals = list(range(12)),
            ticktext = month_names,
            title = 'Monat'
        ),
        yaxis = dict(
            range = [-1, 1],  # Setzen Sie den y-Achsenbereich auf -1 bis 1 für NDVI
            title = 'NDVI'
        ),
        title="NDVI im Jahresverlauf",
        showlegend = False
    )
    
    return fig
    