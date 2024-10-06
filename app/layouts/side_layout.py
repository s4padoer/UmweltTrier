import dash_leaflet as dl
from dash import html, dcc
import pandas as pd
from sqlalchemy import text
from load_data import get_engine
from figures.figure_ndvi import create_empty_ndvi_figure

MAP_ID = "map"
NDVI_ID = "ndvi-barplot"
SITE_ID = "karten-seite"


def get_map():
    engine = get_engine()
    query = text("""select wetterstation.name as stationname, wetterstation.geo_breite, wetterstation.geo_laenge, dienst.name as dienstname
                 from wetterstation
                join dienst on wetterstation.dienst_ident = dienst.ident 
                ;""")
    wetterstationen = pd.read_sql(query, engine)
    markers = []
    for index, station in wetterstationen.iterrows():
        tooltip = "{stationname} \n Liefert Daten an: {dienstname}".format(stationname = station["stationname"], dienstname = station["dienstname"]) 
        marker = dl.Marker(position=[station["geo_breite"], station["geo_laenge"]], 
                           children=[dl.Tooltip(
                                   dcc.Markdown(tooltip, style={'white-space': 'pre-line'}),
                           )])
        markers.append(marker)
    
    # Zentrum von Trier
    center = [49.75, 6.63]
    map = dl.Map(center=center, zoom=13, children=[
        dl.TileLayer(),  # OpenStreetMap als Basiskarte
        dl.LayerGroup(id='layer-group'),
        *markers
        ], style={'width': '100%', 'height': '500px'}, 
        id=MAP_ID
        )

    return map


def get_map_layout(ndvi_figure):
    if ndvi_figure is None:
        ndvi_figure = create_empty_ndvi_figure()
    map = get_map()
    map_layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.H1('Karte von Trier'),
    map,
    html.Div(id={'type': SITE_ID, 'page': 'karte'}),
    dcc.Loading(
    id="loading-ndvi",
    type="default",
    children=[dcc.Graph(id=NDVI_ID, figure=ndvi_figure)]
    ),
    html.Br(),
    dcc.Link('Zurück zur Hauptseite', href='/', className='button')
    ])
    return map_layout