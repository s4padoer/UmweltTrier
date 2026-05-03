from dash import Dash, html, dcc
from dotenv import load_dotenv
from flask import Flask
from threading import Thread


from layouts.main_layout import update_figure
from load_data import listen_for_notifications
from layouts.callbacks import register_callbacks

server = Flask(__name__)
app = Dash(__name__, server=server, suppress_callback_exceptions=True)

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
