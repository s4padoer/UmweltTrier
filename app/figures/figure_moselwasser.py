from sqlalchemy import text
import load_data 

def update_moseltemperatur_und_fisch():
    engine = load_data.get_engine()
    assets_dir = "/assets"
    with engine.connect() as conn:
        query = text("SELECT wert FROM wassertemperatur_mosel WHERE zeitpunkt = (SELECT MAX(zeitpunkt) FROM wassertemperatur_mosel)")
        result = conn.execute(query)
        mosel_temperatur = result.fetchone()[0]
        mosel_temperatur = round(mosel_temperatur,1)
        
    # Hier die aktuelle Temperatur abrufen
    if 10 <= mosel_temperatur <= 20:
        return f'{mosel_temperatur}°C', assets_dir+'/images/happy_fish.jpeg'
    else:
        return f'{mosel_temperatur}°C', assets_dir+'/images/sad_fish.jpeg'
