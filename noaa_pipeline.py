import requests
import pandas as pd
import time
import os
from db import engine

# for API help --> https://www.ncdc.noaa.gov/cdo-web/webservices/v2 
noaa_token = os.environ["noaa_token"]


headers = {"token": noaa_token}
# all 50 states
states = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56"
    }
# 3 datatypes we need
datatypes = ["TAVG", "AWND", "TSUN"]
# chuncks under 10 years 
date_chunks = [
    ("2000-01-01", "2009-12-31"),
    ("2010-01-01", "2019-12-31"),
    ("2020-01-01", "2023-12-31")
    ]

def fetch_noaa_status(state_code, fips, datatype, start, end):
    """
    Fetch all station rows for one state, one datatype, one time chunk,
    returns raw station-level dataframe
    """

    all_rows = []
    offset = 1
    # NOAA limit
    limit = 1000
    while True:
        url = (
            f"https://www.ncei.noaa.gov/cdo-web/api/v2/data"
            f"?datasetid=GSOY"
            f"&datatypeid={datatype}"
            f"&locationid=FIPS:{fips}"
            f"&startdate={start}"
            f"&enddate={end}"
            f"&limit={limit}"
            f"&offset={offset}"
            f"&units=standard"
            )

        response = requests.get(url, headers=headers)
        # handle errors during fetch (aka different datatypes)
        try:
            data = response.json()
        except:
            print(f"Non-JSON ERROR {state_code} {datatype} {start}")
            break
        if "results" not in data:
            print(f"No results for {state_code} {datatype} {start}")
            break
        
        rows = data["results"]
        total = data["metadata"]["resultset"]["count"]
        all_rows.extend(rows)
        # stop when we have everything 
        if len(all_rows) >= total:
            break
        offset += limit
        # delay time between each request
        time.sleep(0.5)

    if not all_rows:
        return pd.DataFrame()

    # convert to dataframe
    df = pd.DataFrame(all_rows)
    # extract year from date column
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["state"] = state_code
    return df[["state", "year", "station", "datatype", "value"]]

all_results = []

for datatype in datatypes:
    print(f"\n{'='*50}")
    print(f"Fetching {datatype}...")
    print(f"{'='*50}")
    for state_code, fips in states.items():
        for start, end in date_chunks:
            print(f" {state_code}|{datatype}|{start[:4]}-{end[:4]}")
            df = fetch_noaa_status(state_code, fips, datatype, start, end)
            if not df.empty:
                all_results.append(df)
            time.sleep(0.2)

# combine everything into one dataframe
df_final = pd.concat(all_results, ignore_index=True)
df_final.to_sql("noaa_climate", engine, if_exists="replace", index=False)
print(f"Shape: {df_final.shape}")
print("noaa_climate saved!")