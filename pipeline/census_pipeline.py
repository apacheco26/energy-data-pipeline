import requests
import pandas as pd
import io
import os
from db import engine, is_fetched, log_fetch

state_pop_key = os.environ["STATE_POP_KEY"]

url_2010s = (
    f"https://api.census.gov/data/2019/pep/population"
    f"?get=NAME,POP,DATE_DESC&for=state:*"
    f"&DATE_CODE=3,4,5,6,7,8,9,10,11,12"
    f"&key={state_pop_key}"
)
url_2000s = (
    f"https://api.census.gov/data/2000/pep/int_population"
    f"?get=POP,DATE_DESC&for=state:*&key={state_pop_key}"
)
url_2020s = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2023/state/totals/NST-EST2023-ALLDATA.csv"
)


def run():
    if is_fetched("census"):
        print("census_population already fetched — skipping")
        return

    try:
        r1 = requests.get(url_2010s, timeout=30)
        r1.raise_for_status()
        r2 = requests.get(url_2000s, timeout=30)
        r2.raise_for_status()
        r3 = requests.get(url_2020s, timeout=30)
        r3.raise_for_status()
    except Exception as e:
        print(f"ERROR fetching Census data: {e}")
        log_fetch("census", "error")
        return

    # Parse 2010s
    raw = r1.json()
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
                "population": int(record["POP"]),
            })

    # Parse 2000s
    raw2 = r2.json()
    header2 = raw2[0]
    fips_to_state = {row["fips"]: row["state"] for row in rows_2010s}
    rows_2000s = []
    for row in raw2[1:]:
        record = dict(zip(header2, row))
        if "7/1/" in record["DATE_DESC"] and "population estimate" in record["DATE_DESC"]:
            year = int(record["DATE_DESC"].split("/")[2].split(" ")[0])
            if 2000 <= year <= 2009:
                fips = record["state"].zfill(2)
                rows_2000s.append({
                    "state": fips_to_state.get(fips),
                    "fips": fips,
                    "year": year,
                    "population": int(record["POP"]),
                })

    # Parse 2020s
    df3 = pd.read_csv(io.StringIO(r3.text))
    df3 = df3[df3["SUMLEV"] == 40]
    rows_2020s = []
    for _, row in df3.iterrows():
        for year in [2020, 2021, 2022, 2023]:
            rows_2020s.append({
                "state": row["NAME"],
                "fips": str(row["STATE"]).zfill(2),
                "year": year,
                "population": int(row[f"POPESTIMATE{year}"]),
            })

    census_pop = pd.DataFrame(rows_2000s + rows_2010s + rows_2020s)
    census_pop = census_pop[["state", "fips", "year", "population"]]
    census_pop = census_pop.sort_values(["state", "year"]).reset_index(drop=True)
    print(census_pop.shape)

    census_pop.to_sql("census_population", engine, if_exists="replace", index=False)
    log_fetch("census", "success")
    print("census_population saved!")
