#Joins surface ownership data with well locations
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import time


stratmap_path = Path(r"C:\Users\milom\Documents\landman\stratmap_countys")
wells_path = Path(r"C:\Users\milom\Documents\landman\permitnormalizer\geodata\wells")
output_geo_folder = Path(r"C:\Users\milom\Documents\landman\wells_with_owner_full")
output_attr_folder = Path(r"C:\Users\milom\Documents\landman\wells_with_owner_attrs")

output_geo_folder.mkdir(exist_ok=True)
output_attr_folder.mkdir(exist_ok=True)

start_time = time.time()


number_to_name = {
    "001": "ANDERSON",
    "003": "ANDREWS",
    "005": "ANGELINA",
    "007": "ARANSAS",
    "009": "ARCHER",
    "011": "ARMSTRONG",
    "013": "ATASCOSA",
    "015": "AUSTIN",
    "017": "BAILEY",
    "019": "BANDERA",
    "021": "BASTROP",
    "023": "BAYLOR",
    "025": "BEE",
    "027": "BELL",
    "029": "BEXAR",
    "031": "BLANCO",
    "033": "BORDEN",
    "035": "BOSQUE",
    "037": "BOWIE",
    "039": "BRAZORIA",
    "041": "BRAZOS",
    "043": "BREWSTER",
    "045": "BRISCOE",
    "047": "BROOKS",
    "049": "BROWN",
    "051": "BURLESON",
    "053": "BURNET",
    "055": "CALDWELL",
    "057": "CALHOUN",
    "059": "CALLAHAN",
    "061": "CAMERON",
    "063": "CAMP",
    "065": "CARSON",
    "067": "CASS",
    "069": "CASTRO",
    "071": "CHAMBERS",
    "073": "CHEROKEE",
    "075": "CHILDRESS",
    "077": "CLAY",
    "079": "COCHRAN",
    "081": "COKE",
    "083": "COLEMAN",
    "085": "COLLIN",
    "087": "COLLINGSWORTH",
    "089": "COLORADO",
    "091": "COMAL",
    "093": "COMANCHE",
    "095": "CONCHO",
    "097": "COOKE",
    "099": "CORYELL",
    "101": "COTTLE",
    "103": "CRANE",
    "105": "CROCKETT",
    "107": "CROSBY",
    "109": "CULBERSON",
    "111": "DALLAM",
    "113": "DALLAS",
    "115": "DAWSON",
    "117": "DEAF SMITH",
    "119": "DELTA",
    "121": "DENTON",
    "123": "DEWITT",
    "125": "DICKENS",
    "127": "DIMMIT",
    "129": "DONLEY",
    "131": "DUVAL",
    "133": "EASTLAND",
    "135": "ECTOR",
    "137": "EDWARDS",
    "139": "ELLIS",
    "141": "EL PASO",
    "143": "ERATH",
    "145": "FALLS",
    "147": "FANNIN",
    "149": "FAYETTE",
    "151": "FISHER",
    "153": "FLOYD",
    "155": "FOARD",
    "157": "FORT BEND",
    "159": "FRANKLIN",
    "161": "FREESTONE",
    "163": "FRIO",
    "165": "GAINES",
    "167": "GALVESTON",
    "169": "GARZA",
    "171": "GILLESPIE",
    "173": "GLASSCOCK",
    "175": "GOLIAD",
    "177": "GONZALES",
    "179": "GRAY",
    "181": "GRAYSON",
    "183": "GREGG",
    "185": "GRIMES",
    "187": "GUADALUPE",
    "189": "HALE",
    "191": "HALL",
    "193": "HAMILTON",
    "195": "HANSFORD",
    "197": "HARDEMAN",
    "199": "HARDIN",
    "201": "HARRIS",
    "203": "HARRISON",
    "205": "HARTLEY",
    "207": "HASKELL",
    "209": "HAYS",
    "211": "HEMPHILL",
    "213": "HENDERSON",
    "215": "HIDALGO",
    "217": "HILL",
    "219": "HOCKLEY",
    "221": "HOOD",
    "223": "HOPKINS",
    "225": "HOUSTON",
    "227": "HOWARD",
    "229": "HUDSPETH",
    "231": "HUNT",
    "233": "HUTCHINSON",
    "235": "IRION",
    "237": "JACK",
    "239": "JACKSON",
    "241": "JASPER",
    "243": "JEFF DAVIS",
    "245": "JEFFERSON",
    "247": "JIM HOGG",
    "249": "JIM WELLS",
    "251": "JOHNSON",
    "253": "JONES",
    "255": "KARNES",
    "257": "KAUFMAN",
    "259": "KENDALL",
    "261": "KENEDY",
    "263": "KENT",
    "265": "KERR",
    "267": "KIMBLE",
    "269": "KING",
    "271": "KINNEY",
    "273": "KLEBERG",
    "275": "KNOX",
    "277": "LAMAR",
    "279": "LAMB",
    "281": "LAMPASAS",
    "283": "LA SALLE",
    "285": "LAVACA",
    "287": "LEE",
    "289": "LEON",
    "291": "LIBERTY",
    "293": "LIMESTONE",
    "295": "LIPSCOMB",
    "297": "LIVE OAK",
    "299": "LLANO",
    "301": "LOVING",
    "303": "LUBBOCK",
    "305": "LYNN",
    "307": "MCCULLOCH",
    "309": "MCLENNAN",
    "311": "MCMULLEN",
    "313": "MADISON",
    "315": "MARION",
    "317": "MARTIN",
    "319": "MASON",
    "321": "MATAGORDA",
    "323": "MAVERICK",
    "325": "MEDINA",
    "327": "MENARD",
    "329": "MIDLAND",
    "331": "MILAM",
    "333": "MILLS",
    "335": "MITCHELL",
    "337": "MONTAGUE",
    "339": "MONTGOMERY",
    "341": "MOORE",
    "343": "MORRIS",
    "345": "MOTLEY",
    "347": "NACOGDOCHES",
    "349": "NAVARRO",
    "351": "NEWTON",
    "353": "NOLAN",
    "355": "NUECES",
    "357": "OCHILTREE",
    "359": "OLDHAM",
    "361": "ORANGE",
    "363": "PALO PINTO",
    "365": "PANOLA",
    "367": "PARKER",
    "369": "PARMER",
    "371": "PECOS",
    "373": "POLK",
    "375": "POTTER",
    "377": "PRESIDIO",
    "379": "RAINS",
    "381": "RANDALL",
    "383": "REAGAN",
    "385": "REAL",
    "387": "RED RIVER",
    "389": "REEVES",
    "391": "REFUGIO",
    "393": "ROBERTS",
    "395": "ROBERTSON",
    "397": "ROCKWALL",
    "399": "RUNNELS",
    "401": "RUSK",
    "403": "SABINE",
    "405": "SAN AUGUSTINE",
    "407": "SAN JACINTO",
    "409": "SAN PATRICIO",
    "411": "SAN SABA",
    "413": "SCHLEICHER",
    "415": "SCURRY",
    "417": "SHACKELFORD",
    "419": "SHELBY",
    "421": "SHERMAN",
    "423": "SMITH",
    "425": "SOMERVELL",
    "427": "STARR",
    "429": "STEPHENS",
    "431": "STERLING",
    "433": "STONEWALL",
    "435": "SUTTON",
    "437": "SWISHER",
    "439": "TARRANT",
    "441": "TAYLOR",
    "443": "TERRELL",
    "445": "TERRY",
    "447": "THROCKMORTON",
    "449": "TITUS",
    "451": "TOM GREEN",
    "453": "TRAVIS",
    "455": "TRINITY",
    "457": "TYLER",
    "459": "UPSHUR",
    "461": "UPTON",
    "463": "UVALDE",
    "465": "VAL VERDE",
    "467": "VAN ZANDT",
    "469": "VICTORIA",
    "471": "WALKER",
    "473": "WALLER",
    "475": "WARD",
    "477": "WASHINGTON",
    "479": "WEBB",
    "481": "WHARTON",
    "483": "WHEELER",
    "485": "WICHITA",
    "487": "WILBARGER",
    "489": "WILLACY",
    "491": "WILLIAMSON",
    "493": "WILSON",
    "495": "WINKLER",
    "497": "WISE",
    "499": "WOOD",
    "501": "YOAKUM",
    "503": "YOUNG",
    "505": "ZAPATA",
    "507": "ZAVALA"
}

# List all well directories
well_dirs = [d for d in wells_path.iterdir() if d.is_dir()]
total_counties = len(well_dirs)  # now works because it's a list

comleted_count = 0
print(f"Total counties to process: {total_counties}")

for well_dir in wells_path.iterdir():
    comleted_count += 1
    county_start_time = time.time()
    if not well_dir.is_dir():
        continue
    county_code = well_dir.name.replace("well", "")
    county_name = number_to_name.get(county_code)
    if not county_name:
        print(f"County code {county_code} not found in mapping.")
        continue

    s_shp_file = list(well_dir.glob("*.shp"))
    if not s_shp_file:
        print(f"No shapefile found in {well_dir}.")
        continue

    well_gdf = gpd.read_file(s_shp_file[0])

    parcel_file = stratmap_path / f"{county_name}.gpkg"
    if not parcel_file.exists():
        print(f"No parcels for {county_name}")
        continue

    parcel_gdf = gpd.read_file(parcel_file)

    if well_gdf.crs != parcel_gdf.crs:
        well_gdf = well_gdf.to_crs(parcel_gdf.crs)

    merged = gpd.sjoin(well_gdf, parcel_gdf, how="left", predicate="within")

       # Save full geodata file
    merged.to_file(output_geo_folder / f"{county_name}_surface_wells_with_owner_full.gpkg", driver="GPKG")

    # Create attribute-only CSV with lat/lon
    merged_attr = merged.drop(columns="geometry").copy()
    merged_attr["LAT"] = merged.geometry.y
    merged_attr["LON"] = merged.geometry.x
    merged_attr.to_csv(output_attr_folder / f"{county_name}_wells_attributes.csv", index=False)

    print(f"Processed {county_name} ({county_code}) — Geo: {len(merged)}, Attr: {len(merged_attr)})")
    county_duration = time.time() - county_start_time
    print(f"Time taken for {county_name}: {county_duration:.2f} seconds")
    print(f"Progress: {comleted_count}/{total_counties} counties processed.")

print(f"Total processing time: {time.time() - start_time:.2f} seconds")
print("Processing complete.")

  