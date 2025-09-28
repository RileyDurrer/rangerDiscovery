import os
import fnmatch
import pandas as pd
import geopandas as gpd

# === STEP 1: Gather all shapefiles ===
base_dir = r'C:\Users\milom\OneDrive\Desktop\landman\permitnormalizer\geodata\documents_20250721'
pattern = 'surv???p.shp'  # Only polygon layers

shapefiles = []
for root, _, files in os.walk(base_dir):
    for file in files:
        if fnmatch.fnmatch(file.lower(), pattern):
            shapefiles.append(os.path.join(root, file))

# === STEP 2: Load and combine shapefiles ===
def extract_county(path):
    return os.path.basename(os.path.dirname(path)).upper().strip()

gdfs = []
for shp in shapefiles:
    gdf = gpd.read_file(shp)
    gdf['COUNTY'] = extract_county(shp)
    gdfs.append(gdf)

combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)

# === STEP 3: Project to a suitable CRS for accurate centroid calculation ===
projected = combined_gdf.to_crs(epsg=3081)  # NAD83 / Texas Centric Albers

# === STEP 4: Calculate centroids ===
projected['geometry'] = projected.geometry.centroid

# === STEP 5: Convert back to WGS84 (Lat/Lon) for web mapping ===
centroids = projected.to_crs(epsg=4326)

# === STEP 6: Explore centroids on a map ===
print(centroids.shape)
print(centroids.head())

m=centroids.explore(
    column='COUNTY',
    tooltip=['COUNTY', 'ABSTRACT_L'],
    marker_type='circle',
    marker_kwds={'radius': 5, 'fill': True},
    legend=False
)
# Save and open manually
m.save('abstract_centroids_map.html')

import webbrowser
webbrowser.open('abstract_centroids_map.html')