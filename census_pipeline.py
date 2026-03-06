import requests
import pandas as pd
import io
import os
import json
from db import engine, save_to_jsonb

state_pop_key = os.environ["STATE_POP_KEY"]

# 2010s
url1 = (
    f"https://api.census.gov/data/2019/pep/population"
    f"?get=NAME,POP,DATE_DESC&for=state:*&key={state_pop_key}"
    )
response = requests.get(url1)
print(response.status_code)


# 2000s
url2 = (
    f"https://api.census.gov/data/2000/pep/int_population"
    f"?get=POP,DATE_DESC&for=state:*&key={state_pop_key}")

response2 = requests.get(url2)
print(response2.status_code)

# Parse 2010s
raw = json.loads(response.text)
header = raw[0]
rows_2010s = []
for row in raw[1:]:
    record = dict(zip(header, row))
    if "7/1/" in record["DATE_DESC"] and "population estimate" in record["DATE_DESC"]:
        year = int(record["DATE_DESC"].split("/")[2].split(" ")[0])
        rows_2010s.append({
            "state": record["NAME"],
            "fips": record["state"].zfill(2),
            "year": year,
            "population": int(record["POP"])
        })

# Parse 2000s
raw2 = json.loads(response2.text)
header2 = raw2[0]
rows_2000s = []
for row in raw2[1:]:
    record = dict(zip(header2, row))
    if "7/1/" in record["DATE_DESC"] and "population estimate" in record["DATE_DESC"]:
        year = int(record["DATE_DESC"].split("/")[2].split(" ")[0])
        if 2000 <= year <= 2009:
            rows_2000s.append({
                "fips": record["state"].zfill(2),
                "year": year,
                "population": int(record["POP"])
            })

# state name mapping from 2010s data
fips_to_state = {row["fips"]: row["state"] for row in rows_2010s}
for row in rows_2000s:
    row["state"] = fips_to_state.get(row["fips"], None)

# 2020s
url3 = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2023/state/totals/NST-EST2023-ALLDATA.csv"
    )

response3 = requests.get(url3)
df3 = pd.read_csv(io.StringIO(response3.text))
df3 = df3[df3["SUMLEV"] == 40]

rows_2020s = []
for _, row in df3.iterrows():
    for year in [2020, 2021, 2022, 2023]:
        rows_2020s.append({
            "state": row["NAME"],
            "fips": str(row["STATE"]).zfill(2),
            "year": year,
            "population": int(row[f"POPESTIMATE{year}"])
        })

# merge all decades
all_rows = rows_2000s + rows_2010s + rows_2020s
census_pop = pd.DataFrame(all_rows)
census_pop = census_pop[["state", "fips", "year", "population"]]
census_pop = census_pop.sort_values(["state", "year"]).reset_index(drop=True)

print(census_pop.shape)
print(census_pop.head())

save_to_jsonb(census_pop, "census_population", engine)
print("census_population saved!")