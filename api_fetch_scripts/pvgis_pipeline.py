import requests
import pandas as pd
import io
import os
import json
import time
from db import engine, check_data_exists, is_fetched, log_fetch

country_coords = {
    "China": (35.8617, 104.1954),
    "United States": (37.0902,  -95.7129),
    "Germany": (51.1657, 10.4515),
    "India": (20.5937, 78.9629),
    "Brazil": (-14.2350, -51.9253),
    "United Kingdom": (55.3781,   -3.4360),
    "Australia":(-25.2744, 133.7751),
    "France":(46.2276,2.2137),
    "Japan":(36.2048, 138.2529),
    "Denmark":(56.2639, 9.5018),
    "Mexico":(23.6345, -102.5528),
    "Canada": (56.1304, -106.3468)
    }

for country, (lat, lon) in country_coords.items():
    country_rows = []
    for year in range(2005, 2024):
        if is_fetched("pvgis", state=country, year=year):
            print(f"Skipping {country} {year} — already fetched")
            continue

        print(f"  {country} — {year}")
        url = (
            f"https://re.jrc.ec.europa.eu/api/v5_2/seriescalc"
            f"?lat={lat}"
            f"&lon={lon}"
            f"&startyear={year}"
            f"&endyear={year}"
            f"&components=1"
            f"&outputformat=json"
            )
        # retry logic to catch error and restarts
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                data = response.json()
                df = pd.DataFrame(data["outputs"]["hourly"])
            
                # convert to numeric
                for col in ["Gb(i)", "Gd(i)", "Gr(i)"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                # aggregate to single annual average rows
                avg_row = {
                    "country": country,
                    "year": year,
                    "ghi": (df["Gb(i)"] + df["Gd(i)"] + df["Gr(i)"]).mean(),
                    "direct_avg": df["Gb(i)"].mean(),
                    "diffuse_avg": df["Gd(i)"].mean()
                }               
                country_rows.append(avg_row)
                log_fetch("pvgis", "success", state=country, year=year)
                break
                                           
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"ERROR {country} {year}: {e}")
                    log_fetch("pvgis", "error", state=country, year=year)
                else:
                    print(f"  Retrying {country} {year} attempt {attempt + 2}...")
                    time.sleep(2)

        time.sleep(1)

    if country_rows:
        df = pd.DataFrame(country_rows)
        mode = "replace" if country == list(country_coords.keys())[0] else "append"
        df.to_sql("pvgis_solar", engine, if_exists=mode, index=False)

