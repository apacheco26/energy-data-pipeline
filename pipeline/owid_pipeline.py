import pandas as pd
from db import engine, is_fetched, log_fetch

OWID_URL = "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv"

COLS = [
    "country", "year", "iso_code",
    "population", "gdp", "energy_per_gdp",
    "electricity_generation", "per_capita_electricity",
    "solar_electricity", "solar_share_elec", "solar_elec_per_capita",
    "wind_electricity", "wind_share_elec", "wind_elec_per_capita",
    "renewables_electricity", "renewables_share_elec",
    "greenhouse_gas_emissions", "carbon_intensity_elec",
]


def run():
    if is_fetched("owid"):
        print("owid already fetched — skipping")
        return

    df = pd.read_csv(OWID_URL)
    df_owid = df[COLS]
    df_owid.to_sql("owid", engine, if_exists="replace", index=False, chunksize=1000)
    log_fetch("owid", "success")
    print(f"owid saved — {df_owid.shape}")
