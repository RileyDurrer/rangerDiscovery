import fiona
from collections import defaultdict
from pathlib import Path

gdb_path = r"C:\Users\milom\Documents\landman\permitnormalizer\geodata\stratmap24-landparcels_48_lp\stratmap24-landparcels_48.gdb"
layer_name = "stratmap24_landparcels_48"
output_folder = Path(r"C:\Users\milom\Documents\landman\stratmap_split")

output_folder.mkdir(exist_ok=True)

# Keep schema to reuse when writing
with fiona.open(gdb_path, layer=layer_name) as src:
    schema = src.schema
    crs = src.crs

    # One writer per county
    writers = {}

    for feat in src:
        county = feat["properties"]["COUNTY"].replace(" ", "_")

        if county not in writers:
            # Create new GeoPackage for this county
            out_path = output_folder / f"{county}.gpkg"
            writers[county] = fiona.open(
                out_path,
                mode="w",
                driver="GPKG",  # or "ESRI Shapefile"
                schema=schema,
                crs=crs,
                layer=county
            )

        writers[county].write(feat)

    # Close all files
    for w in writers.values():
        w.close()
