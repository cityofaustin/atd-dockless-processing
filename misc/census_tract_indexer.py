'''
Take an input geojson grid and:
- add a unique `id` property
- add a `FeatureIndex` element where each entry is an `id` key for each cell
'''
import json
import pdb

finame = "data/census_tracts_2010_simplified_20pct.json"
foutname = "data/census_tracts_2010_simplified_20pct_indexed.json"

with open(finame, "r") as fin:
    data = json.loads(fin.read())

    new_data = {}

    for i, feature in enumerate(data["features"]):

        geoid = feature["properties"]["GEOID10"]
        
        # truncate decimals

        new_data[geoid] = feature

    with open(foutname, "w") as fout:
        fout.write(json.dumps(new_data))
