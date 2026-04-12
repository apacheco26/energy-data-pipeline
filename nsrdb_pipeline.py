import requests
import pandas as pd
import io
import os
import json
import time
from db import engine, check_data_exists, is_fetched, log_fetch

api_key_NREL = os.environ["NREL_API_KEY"]
email_key = os.environ["EMAIL_KEY"]

states_coords = {
    "AL": (32.8, -86.8), "AK": (64.2, -153.0), "AZ": (34.3, -111.1),
    "AR": (34.8, -92.2), "CA": (36.8, -119.4), "CO": (38.9, -105.5),
    "CT": (41.6, -72.7), "DE": (39.0, -75.5),  "FL": (27.8, -81.6),
    "GA": (32.7, -83.4), "HI": (20.3, -156.4), "ID": (44.4, -114.6),
    "IL": (40.0, -89.2), "IN": (39.8, -86.1),  "IA": (42.1, -93.5),
    "KS": (38.5, -98.4), "KY": (37.5, -85.3),  "LA": (31.1, -91.8),
    "ME": (45.3, -69.2), "MD": (39.0, -76.8),  "MA": (42.3, -71.8),
    "MI": (44.3, -85.4), "MN": (46.3, -94.3),  "MS": (32.7, -89.7),
    "MO": (38.4, -92.5), "MT": (46.9, -110.4), "NE": (41.5, -99.9),
    "NV": (39.5, -116.9),"NH": (43.7, -71.6),  "NJ": (40.1, -74.5),
    "NM": (34.4, -106.1),"NY": (42.9, -75.5),  "NC": (35.5, -79.4),
    "ND": (47.4, -100.5),"OH": (40.2, -82.8),  "OK": (35.6, -96.9),
    "OR": (44.0, -120.5),"PA": (40.9, -77.8),  "RI": (41.7, -71.5),
    "SC": (33.9, -80.9), "SD": (44.4, -100.2), "TN": (35.9, -86.3),
    "TX": (31.0, -100.0),"UT": (39.4, -111.1), "VT": (44.0, -72.7),
    "VA": (37.5, -78.9), "WA": (47.4, -120.4), "WV": (38.6, -80.6),
    "WI": (44.5, -89.5), "WY": (43.0, -107.6)
    }



# If errors 
for state, (lat, lon) in states_coords.items(): 
    state_rows = []
    
    for year in range(2000, 2024):
        if is_fetched("nsrdb", state=state, year=year):
            print(f"  Skipping {state} {year} — already fetched")
            continue
    
    print(f" {state} — {year}")
    url = (
        f"https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-aggregated-v4-0-0-download.csv"
        f"?api_key={api_key_NREL}"
        f"&wkt=POINT({lon} {lat})"
        f"&names={year}"
        f"&attributes=ghi,dni"
        f"&leap_day=false"
        f"&utc=false"
        f"&interval=60"
        f"&email={email_key}"
        )
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            lines = response.text.split("\n")
            data_start = next(i for i, l in enumerate(lines) if l.startswith("Year"))
            df = pd.read_csv(io.StringIO("\n".join(lines[data_start:])))
            state_rows.append({
                "state": state,
                "year": year,
                "avg_ghi": round(df["GHI"].mean(), 2),
                "avg_dni": round(df["DNI"].mean(), 2)
            })
            log_fetch("nsrdb", "success", state=state, year=year)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  ERROR {state} {year}: {e}")   
                log_fetch("nsrdb", "error", state=state, year=year)
            else:
                print(f"Retrying {state} {year} attempt {attempt + 2}")
                time.sleep(2)
        time.sleep(1)   
1
if state_rows:
    df_nsrdb = pd.DataFrame(state_rows)
    mode = "replace" if not check_data_exists("nsrdb_solar") else "append"
    df_nsrdb.to_sql("nsrdb_solar", engine, if_exists=mode, index=False)