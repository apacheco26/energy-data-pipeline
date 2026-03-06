import requests
import pandas as pd
import io
import os
import json
import time
from db import engine, save_to_jsonb

target_countries = [
    "China", "Germany", "India", "Brazil",
    "United Kingdom", "Australia", "France", "Japan", "Denmark",
    "Mexico", "Canada"]


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

pvgis_rows = []

for country, (lat, lon) in country_coords.items():
    for year in range(2005, 2024):
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
        
        try:
            response = requests.get(url)
            data = response.json()
            df = pd.DataFrame(data["outputs"]["hourly"])
            df["country"] = country
            df["year"] = year
            pvgis_rows.append(df[["country", "year", "time", "Gb(i)", "Gd(i)", "Gr(i)"]])
        except Exception as e:
            print(f"ERROR {country} {year}: {e}")
        time.sleep(1)

df_pvgis = pd.concat(pvgis_rows, ignore_index=True)
save_to_jsonb(df_pvgis,"pvgis_solar_raw",engine)
print(f"Done! {df_pvgis.shape}")
