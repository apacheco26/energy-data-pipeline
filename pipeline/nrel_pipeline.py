import requests
import pandas as pd
import time
import os
from db import engine

api_key_NREL = os.environ["NREL_API_KEY"]

# Representative lat/lon per state
states_coords = {
    "AL": (32.8, -86.8),  "AK": (64.2, -153.0), "AZ": (34.3, -111.1),
    "AR": (34.8, -92.2),  "CA": (36.8, -119.4),  "CO": (38.9, -105.5),
    "CT": (41.6, -72.7),  "DE": (39.0, -75.5),   "FL": (27.8, -81.6),
    "GA": (32.7, -83.4),  "HI": (20.3, -156.4),  "ID": (44.4, -114.6),
    "IL": (40.0, -89.2),  "IN": (39.8, -86.1),   "IA": (42.1, -93.5),
    "KS": (38.5, -98.4),  "KY": (37.5, -85.3),   "LA": (31.1, -91.8),
    "ME": (45.3, -69.2),  "MD": (39.0, -76.8),   "MA": (42.3, -71.8),
    "MI": (44.3, -85.4),  "MN": (46.3, -94.3),   "MS": (32.7, -89.7),
    "MO": (38.4, -92.5),  "MT": (46.9, -110.4),  "NE": (41.5, -99.9),
    "NV": (39.5, -116.9), "NH": (43.7, -71.6),   "NJ": (40.1, -74.5),
    "NM": (34.4, -106.1), "NY": (42.9, -75.5),   "NC": (35.5, -79.4),
    "ND": (47.4, -100.5), "OH": (40.2, -82.8),   "OK": (35.6, -96.9),
    "OR": (44.0, -120.5), "PA": (40.9, -77.8),   "RI": (41.7, -71.5),
    "SC": (33.9, -80.9),  "SD": (44.4, -100.2),  "TN": (35.9, -86.3),
    "TX": (31.0, -100.0), "UT": (39.4, -111.1),  "VT": (44.0, -72.7),
    "VA": (37.5, -78.9),  "WA": (47.4, -120.4),  "WV": (38.6, -80.6),
    "WI": (44.5, -89.5),  "WY": (43.0, -107.6),
}


def run():
    """Fetch NREL annual solar resource summary per state and save to nrel_solar."""
    rows = []
    for state, (lat, lon) in states_coords.items():
        print(f"  Fetching NREL data for {state}...")
        url = (
            f"https://developer.nrel.gov/api/solar/solar_resource/v1.json"
            f"?api_key={api_key_NREL}"
            f"&lat={lat}"
            f"&lon={lon}"
        )
        try:
            response = requests.get(url)
            data = response.json()
        except Exception:
            print(f"  Error fetching {state}")
            continue

        if data.get("errors"):
            print(f"  API error for {state}: {data['errors']}")
            continue

        outputs = data["outputs"]
        if not all(isinstance(outputs.get(k), dict) for k in ("avg_ghi", "avg_dni", "avg_lat_tilt")):
            print(f"  Skipping {state} — incomplete data")
            continue

        rows.append({
            "state": state,
            "lat": lat,
            "lon": lon,
            "avg_ghi_annual": outputs["avg_ghi"]["annual"],
            "avg_dni_annual": outputs["avg_dni"]["annual"],
            "avg_lat_tilt": outputs["avg_lat_tilt"]["annual"],
        })
        time.sleep(0.5)

    if rows:
        df_nrel = pd.DataFrame(rows)
        df_nrel.to_sql("nrel_solar", engine, if_exists="replace", index=False)
        print(f"nrel_solar saved — {len(df_nrel)} rows")
