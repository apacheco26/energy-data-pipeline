import requests
import pandas as pd
import time
import os
from db import engine, check_data_exists, is_fetched, log_fetch

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
    chunk_year = int(start[:4])
    
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
        max_retries = 3
        for attempt in range(max_retries):
            response = requests.get(url, headers=headers, timeout=30)
            # handle errors during fetch (aka different datatypes)
            try:
                data = response.json()
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Non-JSON ERROR {state_code} {datatype} {start}: {e}")
                    log_fetch("noaa", "error", state=state_code, year=chunk_year)
                    return pd.DataFrame()
                time.sleep(2)
                
        if "results" not in data:
            print(f"No results for {state_code} {datatype} {start}")
            log_fetch("noaa", "no_data", state=state_code, year=chunk_year)
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

first_write = True

for datatype in datatypes:
    print(f"\n{'='*50}")
    print(f"Fetching {datatype}...")
    print(f"{'='*50}")
    for state_code, fips in states.items():
        for start, end in date_chunks:
            chunk_year = int(start[:4])
            if is_fetched("noaa", state=state_code, year=chunk_year):
                print(f"  Skipping {state_code} {datatype} {start[:4]} — already fetched")
                continue
            
            print(f" {state_code}|{datatype}|{start[:4]}-{end[:4]}")
            df = fetch_noaa_status(state_code, fips, datatype, start, end)
            if not df.empty:
                
                mode = "replace" if first_write else "append"
                df.to_sql("noaa_climate", engine, if_exists=mode, index=False)
                first_write = False
                
                log_fetch("noaa", "success", state=state_code, year=chunk_year)

print("noaa_climate saved!")