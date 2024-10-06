import datetime as dt
import calendar
from collections import defaultdict

import rasterio.warp
from eodag import EODataAccessGateway
from shapely.geometry import shape, mapping
from shapely import wkt, box
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.features import geometry_mask
from sqlalchemy import text
import sys
import yaml
import json
import os
import re
from pyproj import Transformer
import numpy as np
import pyproj
import shutil
import geopandas as gpd

from write_to_database import write_to_database
#############################################
# Laden der Parameter
#############################################
# Laden der separaten YAML-Datei
with open('assets/eodag.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)
    config_string = yaml.dump(config)


from load_data import get_engine
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


# Transformer erstellen
transformer = Transformer.from_crs("EPSG:4326", "EPSG:32632", always_xy=True)

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
def transform_coords(coords):
    return [transformer.transform(x, y) for x, y in coords]

def covers_geojson(product, geojson_polygon):
    product_bbox = box(*product.geometry.bounds)
    return product_bbox.contains(geojson_polygon)

def calculate_ndvi(red_band_path, nir_band_path):
        # Bänder laden und NDVI berechnen
    with rasterio.open(red_band_path) as red:
        red_band = red.read(1)
        src_transform = red.transform
        src_crs = red.crs
        profile = red.profile.copy()
    with rasterio.open(nir_band_path) as nir:
        nir_band = nir.read(1)
        
       # NDVI berechnen
    ndvi = (nir_band.astype(float) - red_band.astype(float)) / (nir_band + red_band)
    scaled_data = ((-ndvi + 1) / 2 * 65535).astype(np.uint16)

    # Reprojektion und Masking
    dst_crs = pyproj.CRS.from_epsg(4326)  # Ziel-CRS (WGS84)
    
    # Berechnen der Transformation für das gesamte Bild
    full_dst_transform, full_dst_width, full_dst_height = calculate_default_transform(
        src_crs, dst_crs, red_band.shape[1], red_band.shape[0], *rasterio.transform.array_bounds(red_band.shape[0], red_band.shape[1], src_transform)
    )
    
    # Reprojektion des gesamten Bildes
    full_resampled = np.zeros((1, full_dst_height, full_dst_width), dtype=np.uint16)
    reproject(
        source=scaled_data,
        destination=full_resampled,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=full_dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.bilinear
    )

    return full_resampled, full_dst_transform, profile

def write_image(image_data, transform, geometry, profile, filepath):    
    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(driver='GTiff', 
                      height=image_data.shape[1], 
                      width=image_data.shape[2], 
                      count=1, 
                      dtype=image_data.dtype, 
                      crs=profile['crs'], 
                      transform=transform) as dataset:
            dataset.write(image_data[0], 1)
    
        with memfile.open() as dataset:
            out_image, out_transform = mask(dataset, [geometry], crop=False, all_touched=True)
            
        # Erstellen Sie das Profil für die Ausgabedatei
    profile.update({
            'driver': 'GTiff',
            'height': out_image.shape[1],
            'width': out_image.shape[2],
            'transform': out_transform,
            'dtype': 'uint16',
            'count': 1,
            'compress': 'lzw'
    })
        
    with rasterio.open(filepath, 'w', **profile) as dst:
        dst.write(out_image[0], 1)
    
    
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
geometry_resolution = geojson["features"][0]["properties"]["resolution"]

    
for result in search_results:
    if not covers_geojson(result, geometry):
        continue
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
    # NDVI speichern als GeoTIFF
    ndvi_image, transform, profile = calculate_ndvi(re, nir_band_path, match.group(1))
    ndvi_output_path = os.path.join(download_pfad, f"ndvi_{match.group(1)}.tif")

    write_image(ndvi_image, transform, geometry, profile, ndvi_output_path)    
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
    

def read_and_align_tiff(file_path, ref_transform, polygon, target_shape=(917, 1167)):
    with rasterio.open(file_path) as src:
        data = src.read(1)
        profile = src.profile
        dst_data = np.zeros( (1,target_shape[0], target_shape[1]), dtype=np.uint16)
        
        reproject(
            source=data,
            destination=dst_data,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=ref_transform,
            dst_crs=src.crs,
            dst_shape=target_shape,
            resampling=Resampling.bilinear
        )
        
        mask = geometry_mask([polygon], out_shape=target_shape, transform=ref_transform, invert=True)
        
        # Anwenden der Maske
        shape = dst_data.shape
        dst_data = np.where(mask, dst_data[0], 65535)  # oder verwenden Sie einen anderen NoData-Wert anstelle von np.nan
        dst_data = dst_data.reshape(shape)
        profile["width"] = target_shape[0]
        profile["height"] = target_shape[1]
        profile["transform"] = ref_transform
        return dst_data, profile

def get_reference_bounds(geojson_path):
    gdf = gpd.read_file(geojson_path)
    return gdf.total_bounds

# Definieren Sie den Referenzbereich und die Transformation
ref_bounds = get_reference_bounds("assets/trier.geojson")
ref_transform = rasterio.transform.from_bounds(*ref_bounds, width=917, height=1167)  # Passen Sie width und height an

# Hauptschleife
for file in files:
    if not file.endswith(".tif") or not file.startswith("ndvi_"):
        print(file)
        continue
    
    datum = get_date_from_filename(file)
    datum_string = dt.datetime.strftime(datum, "%Y_%m")
    file_path = os.path.join(download_pfad, file)
    
    # Lesen und Ausrichten des Bildes
    aligned_data, profile = read_and_align_tiff(file_path, ref_transform, geometry)
    
    previous_data = dic[datum_string]
    previous_data.append(aligned_data)
    dic[datum_string] = previous_data
    print(file)
        
profile['nodata'] = 65535

for year_month, data_list in dic.items():
    avg_data = np.mean(data_list, axis=0)
        
    # Speichern Sie das gemittelte Bild
    output_file = os.path.join(download_pfad, f'avg_ndvi_{year_month}.tiff')
    with rasterio.open(output_file, 'w', **profile) as dst:
        dst.write(avg_data)

    # Schreibe in die Datenbank:
    write_to_database(output_file)

