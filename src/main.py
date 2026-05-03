from flask import Flask
from dash import Dash, html

server = Flask(__name__)

@server.route("/")
def index():
    return "Hallo, Vercel lädt mich jetzt!"

app = Dash(
    __name__,
    server=server,
    routes_pathname_prefix="/",
)

app.layout = html.Div("Hallo Dash auf Vercel!")