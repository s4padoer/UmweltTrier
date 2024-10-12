import datetime as dt
import calendar
from collections import defaultdict

import rasterio.transform
import rasterio.warp
from eodag import EODataAccessGateway
from shapely.geometry import shape
from shapely import wkt, box
import rasterio
from rasterio.mask import mask
from rasterio.transform import from_bounds
from rasterio.warp import calculate_default_transform, reproject, Resampling
from sqlalchemy import text
import sys
import yaml
import json
import os
import re
import numpy as np
import pyproj
import shutil

from write_to_database import write_to_database
from load_data import get_engine

#############################################
# Laden der Parameter
#############################################
# Laden der separaten YAML-Datei
with open('assets/eodag.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)
    config_string = yaml.dump(config)

engine = get_engine()

with engine.connect() as conn:
        query = text("SELECT jahr, monat FROM ndvi ORDER BY jahr DESC, monat DESC LIMIT 1")
        result = conn.execute(query)
        lastDate = result.fetchone()
        result = conn.execute(text("SELECT MAX(ident) FROM ndvi"))
        lastIdent = result.fetchone()[0]

heute = dt.datetime.now().date

if lastDate[1] == 12:
    nextMonth = 1
    year = lastDate[0]+1
else:
    nextMonth = lastDate[1] +1
    year = lastDate[0]

if heute.month == nextMonth:
    sys.exit()
    
startdate = dt.date(year, nextMonth, 1)
last_day = calendar.monthrange(startdate.year, startdate.month)[1]
enddate = dt.date(year, nextMonth, last_day)

# Geodaten laden
with open('assets/trier.geojson', 'r') as f:
    geojson = json.load(f)
    
# Geometrie extrahieren und in Shapely-Objekt umwandeln
geometry = shape(geojson['features'][0]['geometry'])
wkt_string = wkt.dumps(geometry)



##########################################################
# Definiere Funktionen
#########################################################
def find_files_with_extension(directory, extension):
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(extension):
                matching_files.append(os.path.join(root, file))
    return matching_files

# Funktion zum Transformieren der Koordinaten
def covers_geojson(product, geojson_polygon):
    product_bbox = box(*product.geometry.bounds)
    return product_bbox.contains(geojson_polygon)

def calculate_ndvi(red_band_path, nir_band_path):
        # Bänder laden und NDVI berechnen
    with rasterio.open(red_band_path) as red:
        red_band = red.read(1)
        profile = red.profile.copy()
        bbox = red.bounds
    with rasterio.open(nir_band_path) as nir:
        nir_band = nir.read(1)
        
       # NDVI berechnen
    ndvi = (nir_band.astype(float) - red_band.astype(float)) / (nir_band + red_band)
    scaled_data = ((-ndvi + 1) / 2 * 65535).astype(np.uint16)
    return scaled_data, profile, bbox


def reproject_image_data(scaled_data, profile, bbox, ausschnitt):
    # Reprojektion und Masking
    dst_crs = pyproj.CRS.from_epsg(4326) 
    src_height, src_width = scaled_data.shape[-1], scaled_data.shape[-2]

    # Berechnen der Transformation für das gesamte Bild
    full_dst_transform, full_dst_width, full_dst_height = calculate_default_transform(
        profile["crs"], dst_crs, src_width, src_height, 
        left=bbox.left, right=bbox.right, bottom=bbox.bottom, top = bbox.top
    )
    # Reprojektion des gesamten Bildes
    full_resampled = np.zeros((1, full_dst_height, full_dst_width), dtype=np.uint16)
    reproject(
        source=scaled_data,
        destination=full_resampled,
        src_transform=profile["transform"],
        src_crs=profile["crs"],
        dst_transform=full_dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.bilinear
    )
    kwargs = profile.copy()
    kwargs.update(
        driver="GTiff",
        crs = dst_crs,
        transform = full_dst_transform,
        height = full_dst_height,
        width = full_dst_width
    )
    print(full_resampled.shape)
    # Führen Sie den Masking-Prozess 
    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(driver='GTiff', 
                      height=full_resampled.shape[1], 
                      width=full_resampled.shape[2], 
                      count=1, 
                      dtype=full_resampled.dtype, 
                      crs=kwargs['crs'], 
                      transform=kwargs["transform"]) as dataset:
            dataset.write(full_resampled[0], 1)
            
        with memfile.open() as src:
            out_image, out_transform = mask(src, [ausschnitt], nodata=65535, 
                                            crop=True, all_touched=True,
                                            pad=True)
            height, width = out_image.shape[1:] 
            new_transform = from_bounds(*ausschnitt.bounds, width, height)
            print(out_image.shape)
            # Aktualisieren Sie die Metadaten
            kwargs.update(
                nodata = 65535,
                height = height,
                width = width,
                transform = out_transform
            )
            return out_image, kwargs
    
def write_image(image_data, profile, filepath):  
    profile.update(
        driver = "GTiff",
        compress="lzw"
    )
    with rasterio.open(filepath, 'w', driver='GTiff', 
                      height=profile["height"], 
                      width=profile["width"], 
                      count=1, 
                      dtype=profile["dtype"], 
                      crs=profile['crs'], 
                      transform=profile["transform"]) as dst:
        dst.write(image_data)
    
    
##########################################################
# Lade Daten herunter 
###########################################################
# Initialisieren Sie den Gateway
dag = EODataAccessGateway()
dag.set_preferred_provider("cop_dataspace")
dag.update_providers_config(config_string)

# Suche durchführen
search_results, total_count = dag.search(
    productType="S2_MSI_L2A",
    geom=wkt_string,
    start=startdate.strftime("%Y-%m-%d"),
    end=enddate.strftime("%Y-%m-%d"),
    cloudCover=30,  # Wolkenbedeckung zwischen 0% und 30%
)

download_pfad = "downloads/copernicus/"

for result in search_results:
    if not covers_geojson(result, geometry):
        continue
    # Immer dieselbe Region verwenden, damit wir spaeter die Bilder nicht nochmal
    # alignen muessen
    if result.properties["title"].rfind("T32ULA")<0:
        continue
    print(result.properties["title"])
    # Download als GeoTIFF
    product_path = dag.download(
        result, extract=True,
        outputs_prefix = download_pfad,
        output_format="GEOTIFF",
        asset=["B04", "B08"],
        geometry=geometry
    )

    red_band_path = find_files_with_extension(os.path.join(product_path, 'GRANULE'), "B04_10m.jp2")[0]
    nir_band_path = find_files_with_extension(os.path.join(product_path, 'GRANULE'), "B08_10m.jp2")[0]
    pattern = r'(\d{8})T'
    download_name = os.path.basename(product_path)
    match = re.search(pattern, download_name.split("_")[-1])
    # NDVI berechnen
    ndvi_image, profile, bbox = calculate_ndvi(red_band_path, nir_band_path)
    ndvi_output_path = os.path.join(download_pfad, f"ndvi_{match.group(1)}.tif")
    # Transformiere zur CRS, die die bisherigen Bilder in der DB haben
    ndvi_reprojected, profile_reprojected = reproject_image_data(ndvi_image, profile, bbox, geometry)
    write_image(ndvi_reprojected, profile_reprojected, ndvi_output_path)    
    product_path = product_path.replace(os.path.basename(product_path), "")
    shutil.rmtree(product_path)

###########################################################
## Bestimme jetzt den Mittelwert
###########################################################

files = os.listdir(download_pfad)
dic = defaultdict(list)

def get_date_from_filename(filename):
    dateformat = "%Y%m%d"
    pattern = r'\d{8}'
    try:
        match = re.search(pattern, filename)
        datum = dt.datetime.strptime(match.group(), dateformat)
        return datum
    except:
        print("Formatierungsfehler")
    
    
# Hauptschleife fuer den Mittelwert
for file in files:
    if not file.endswith(".tif") or not file.startswith("ndvi_"):
        print(f"{file} nicht verarbeitet")
        continue
    
    datum = get_date_from_filename(file)
    datum_string = dt.datetime.strftime(datum, "%Y_%m")
    file_path = os.path.join(download_pfad, file)
    with rasterio.open(file_path, 'r') as src:
        new_data = src.read()
        profile = src.profile
    previous_data = dic[datum_string]
    previous_data.append(new_data)
    dic[datum_string] = previous_data
    print(f"{file} verarbeitet")
        
for year_month, data_list in dic.items():
    avg_data = np.mean(data_list, axis=0)
        
    # Speichern Sie das gemittelte Bild
    output_file = os.path.join(download_pfad, f'avg_ndvi_{year_month}.tiff')
    with rasterio.open(output_file, 'w', **profile) as dst:
        dst.write(avg_data)

    # Schreibe in die Datenbank:
    write_to_database(output_file)

