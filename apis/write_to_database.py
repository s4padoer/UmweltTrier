import subprocess
import os
import re
import rasterio
import json

def remove_first_x_lines(text, x):
    # Den Text in Zeilen aufteilen
    lines = text.splitlines()
    # Die ersten zwei Zeilen entfernen und die restlichen Zeilen wieder zusammenfügen
    return '\n'.join(lines[x:])

def write_to_database(file_path):
    filename = os.path.basename(file_path)
    
    jahr_datum = re.search(r'\d{4}_\d{2}', filename)
    jahr, monat = map(int, jahr_datum.group().split("_"))
    
    with rasterio.open(file_path) as src:
        # Lesen des gesamten Bildes
        data = src.read(1)
        shape = data.shape
        if len(shape) == 3:
            shape = shape[1:]
        print(shape)
    # Schritt 1: Temporäres SQL-Skript erstellen
    temp_sql_file = 'temp_raster.sql'
    raster2pgsql_command = (
        f"raster2pgsql -s 4326 -t {shape[1]}x{shape[0]} -F -I -C -M -b 1 {os.path.relpath(file_path)} public.ndvi > {temp_sql_file}"
    )
    
    subprocess.run(raster2pgsql_command, shell=True, check=True)

    # Schritt 2: SQL-Skript modifizieren
    with open(temp_sql_file, 'r') as file:
        sql_content = file.read()
        sql_content = remove_first_x_lines(sql_content,2)
        sql_content = sql_content.replace(
        f"INSERT INTO \"public\".\"ndvi\" (\"rast\",\"filename\") VALUES",
        f"INSERT INTO ndvi (rast, dateiname, jahr, monat, produkt_ident) VALUES"
        )
        sql_content = sql_content.replace(
            ");",
            f", {jahr}, {monat}, 4);"
        )

        
    # Schreiben Sie das modifizierte SQL in eine neue Datei
    modified_sql_file = 'modified_raster.sql'
    with open(modified_sql_file, 'w') as file:
        file.write(sql_content)

    # Schritt 3: Modifiziertes SQL ausführen
    with open("apis/datenbank.json", "r") as configfile:
        config = json.load(configfile)
        user = config["user"]
    
    os.environ["PGPASSWORD"] = config["password"]
    psql_command = (
        f"psql -d klimatrier -h localhost -U {user} -f {modified_sql_file}"
    )
    subprocess.run(psql_command, shell=True, check=True)
    # Bereinigen Sie temporäre Dateien
    os.remove(temp_sql_file)
    os.remove(modified_sql_file)
