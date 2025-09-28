import fiona
from collections import defaultdict
import pandas as pd

gdb_path = r"C:\Users\milom\Documents\landman\permitnormalizer\geodata\stratmap24-landparcels_48_lp\stratmap24-landparcels_48.gdb"
layer_name = "stratmap24_landparcels_48"

# dict of {county: {"blank": int, "total": int}}
county_counts = defaultdict(lambda: {"blank": 0, "total": 0})

with fiona.open(gdb_path, layer=layer_name) as src:
    for feat in src:
        county = feat["properties"]["COUNTY"]
        owner = feat["properties"]["OWNER_NAME"]

        county_counts[county]["total"] += 1
        if not owner or not owner.strip():
            county_counts[county]["blank"] += 1

# Convert to DataFrame
rows = []
for county, stats in county_counts.items():
    total = stats["total"]
    blank = stats["blank"]
    pct_blank = (blank / total) * 100 if total > 0 else 0
    rows.append((county, total, blank, pct_blank))

df = pd.DataFrame(rows, columns=["COUNTY", "TOTAL_PARCELS", "BLANK_OWNERS", "PCT_BLANK"]).sort_values("PCT_BLANK", ascending=False)

# Statewide total
state_total = df["TOTAL_PARCELS"].sum()
state_blank = df["BLANK_OWNERS"].sum()
state_pct_blank = (state_blank / state_total) * 100

print(df)
print(f"\nStatewide blank owner rate: {state_pct_blank:.2f}%")
