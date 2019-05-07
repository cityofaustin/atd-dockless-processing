'''
Take an input geojson grid and:
- add a unique `id` property
- add a `FeatureIndex` element where each entry is an `id` key for each cell
'''
import json
import pdb

finame = "grid.json"
foutname = "grid_indexed.json"

with open(finame, "r") as fin:
    data = json.loads(fin.read())

    data["FeatureIndex"] = {}

    for i, feature in enumerate(data["features"]):

        # add an id prop
        i = i + 1
        _id = str(i).zfill(6)
        feature["properties"]["id"] = _id
        
        # truncate decimals
        for poly in feature["geometry"]["coordinates"]:
            feature["geometry"]["coordinates"] = [[
                [float(format(elem, "2.6f")) for elem in coord] for coord in poly
            ]]

        data["FeatureIndex"][_id] = feature

    with open(foutname, "w") as fout:
        fout.write(json.dumps(data))
