from sqlalchemy import text
import load_data 

def update_moseltemperatur_und_fisch():
    assets_dir = "/assets"
    query = text("SELECT wert, zeitpunkt FROM wassertemperatur_mosel WHERE zeitpunkt = (SELECT MAX(zeitpunkt) FROM wassertemperatur_mosel)")
    result = load_data.make_query(query)
    (mosel_temperatur, datum) = result.fetchone()
    mosel_temperatur = round(mosel_temperatur,1)
    datumString = datum.strftime("%d.%m.%y")
    # Hier die aktuelle Temperatur abrufen
    if 10 <= mosel_temperatur <= 20:
        return f'{mosel_temperatur}°C ({datumString})', assets_dir+'/images/happy_fish.jpeg'
    else:
        return f'{mosel_temperatur}°C ({datumString})', assets_dir+'/images/sad_fish.jpeg'

