from dash import Dash, html, dcc
from dotenv import load_dotenv
from pathlib import Path
from threading import Thread

# Lädt .env aus dem Projektroot
load_dotenv(Path(__file__).parent.parent.parent.parent / '.env')

from umwelttrier.app.layouts.main_layout import update_figure
from umwelttrier.app.load_data import listen_for_notifications
from umwelttrier.app.layouts.callbacks import register_callbacks

app = Dash(__name__,suppress_callback_exceptions=True)

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
