import json

with open("json_files/states.json") as f:
    states = json.load(f)

with open("json_files/geojson.json") as g:
    geo = json.load(g)

for feature in geo["features"]:
    name = feature["properties"]["name"]
    feature["properties"]["composite_alignment"] = states.get(name)

with open("json_files/us_states_enriched.geojson", "w") as f:
    json.dump(geo, f)

print("donezo")