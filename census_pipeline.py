import requests
import pandas as pd
import io
import os
from db import engine, check_data_exists, is_fetched, log_fetch

state_pop_key = os.environ["STATE_POP_KEY"]

if is_fetched("census"):
    print("census_population already fetched — skipping")

else:
    # 2010s
    url1 = (
        f"https://api.census.gov/data/2019/pep/population"
        f"?get=NAME,POP,DATE_DESC&for=state:*&key={state_pop_key}"
    )
    # 2000s
    url2 = (
        f"https://api.census.gov/data/2000/pep/int_population"
        f"?get=POP,DATE_DESC&for=state:*&key={state_pop_key}"
    )
    # 2020s
    url3 = (
        "https://www2.census.gov/programs-surveys/popest/datasets/"
        "2020-2023/state/totals/NST-EST2023-ALLDATA.csv"
    )

    try:
        response = requests.get(url1, timeout=30)
        response.raise_for_status()
        print(f"url1 status: {response.status_code}")
    except Exception as e:
        print(f"ERROR fetching url1: {e}")
        log_fetch("census", "error")
        exit()

    try:
        response2 = requests.get(url2, timeout=30)
        response2.raise_for_status()
        print(f"url2 status: {response2.status_code}")
    except Exception as e:
        print(f"ERROR fetching url2: {e}")
        log_fetch("census", "error")
        exit()

    try:
        response3 = requests.get(url3, timeout=30)
        response3.raise_for_status()
        print(f"url3 status: {response3.status_code}")
    except Exception as e:
        print(f"ERROR fetching url3: {e}")
        log_fetch("census", "error")
        exit()

    # Parse 2010s
    raw = response.json()
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
    raw2 = response2.json()
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
        row["state"] = fips_to_state.get(row["fips"])
        if row["state"] is None:
            print(f"WARNING — no state name found for fips {row['fips']}")

    # Parse 2020s
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

    census_pop.to_sql("census_population", engine, if_exists="replace", index=False)
    log_fetch("census", "success")
    print("census_population saved!")