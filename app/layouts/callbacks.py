from dash.dependencies import Input, Output, State
import dash
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from layouts.main_layout import mainlayout
from layouts.side_layout import get_map_layout
from figures.figure_ndvi import get_ndvi_hist, create_empty_ndvi_figure
from layouts.side_layout import MAP_ID, NDVI_ID

static_ndvi_figure = create_empty_ndvi_figure()

def register_callbacks(app):
    @app.callback(
        Output('page-content', 'children'),
        Input('url', 'pathname')
    )
    def update_content(pathname):
        # Logik für die Hauptseite
        if pathname == '/':
            return mainlayout
        # Logik für die Kartenansicht
        elif pathname == '/karte':
            return get_map_layout(static_ndvi_figure)  # Keine Aktualisierung für die anderen Outputs
        return dash.no_update  # Standardfall
    
    
    @app.callback(
        Output(NDVI_ID, 'figure'),
        Input(MAP_ID, 'clickData'),
        State(NDVI_ID, "figure"),
        prevent_initial_call=True
    )
    def update_ndvi(clickData, figure):
        if not clickData or 'latlng' not in clickData:
            raise PreventUpdate
        latlondict = clickData["latlng"]
        lat, lon = latlondict["lat"], latlondict["lng"]
        global static_ndvi_figure
        static_ndvi_figure = get_ndvi_hist(lat, lon, static_ndvi_figure)
        return static_ndvi_figure
    
    
