from dash import Dash, html, dcc
from dotenv import load_dotenv
from flask import Flask
from threading import Thread


from .layouts.main_layout import update_figure
from .utils.load_data import listen_for_notifications
from .layouts.callbacks import register_callbacks
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State

server = Flask(__name__)
app = Dash("Umwelt- und Klimadaten aus Trier", server=server, suppress_callback_exceptions=True, assets_folder='assets',
           external_scripts=[
        'https://cdn.plot.ly/plotly-latest.min.js'
    ])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='previous-pathname'),
    html.Div(id='page-content')
])
register_callbacks(app)



if __name__ == '__main__':
    notification_thread = Thread(target=listen_for_notifications, args=(update_figure,))
    notification_thread.daemon = True
    notification_thread.start()
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=8050)
