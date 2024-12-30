from dash import html, dcc
from figures.figure_wetterdienst import get_timeseries_temperatur
from figures.figure_luftqualitaet import get_luftqualitaet_plot
from figures.figure_moselwasser import update_moseltemperatur_und_fisch

VERKEHRSPLOT_STATUS_ID = 'verkehrsplot-status'
VERKEHRSPLOT_BUTTON_ID = 'verkehrsdaten-button'
VERKEHRSPLOT_STATUS_OHNE_VERKEHR = 'ohne_verkehr'
VERKEHRSPLOT_STATUS_MIT_VERKEHR = 'mit_verkehr'

luftqualitaet_plot = get_luftqualitaet_plot()
temperatur_plot = get_timeseries_temperatur()
mosel_temp, fisch_image = update_moseltemperatur_und_fisch()


def get_main_layout():
    layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.H1('Klimadaten für Trier'),
        html.Div([
            html.H2('Temperatur und Niederschlag'),
            dcc.Graph(id='temperatur-graph', figure=temperatur_plot),
        ]),
        html.Div([
            html.H2('Aktuelle Mosel-Temperatur:'),
            html.H3(mosel_temp),  # Temperatur direkt setzen
            html.Img(src=fisch_image, style={'width': '100px', 'height': '100px'})  # Fischbild direkt setzen
        ]),
        html.Div([
            html.H2('Luftqualität:'),
            dcc.Graph(id="luftqualitaet-graph", figure=luftqualitaet_plot),
            html.Button(id=VERKEHRSPLOT_BUTTON_ID, n_clicks=0),
            dcc.Store(id=VERKEHRSPLOT_STATUS_ID, data=VERKEHRSPLOT_STATUS_OHNE_VERKEHR),
        ]),
        html.Br(),
        dcc.Link('Zur Karte', href='/karte', className='button')
    ])
    return layout

def update_figure():
    update_figure(temperatur_plot)
    mosel_temp, fisch_image = update_moseltemperatur_und_fisch()
    main_layout = get_main_layout(temperatur_plot, mosel_temp, fisch_image)
    

mainlayout = get_main_layout()

