from dash import Dash, html, dcc
from threading import Thread
from layouts.main_layout import update_figure, mainlayout
from load_data import get_engine, listen_for_notifications
from layouts.callbacks import register_callbacks

app = Dash(__name__,suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])
register_callbacks(app)



if __name__ == '__main__':
    engine = get_engine()
    notification_thread = Thread(target=listen_for_notifications, args=(update_figure,))
    notification_thread.daemon = True
    notification_thread.start()
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=8050)
