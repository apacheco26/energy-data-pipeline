import requests
import pandas as pd
import os
from db import engine, is_fetched, log_fetch

bea_key = os.environ["BEA_KEY"]

state_fips = {
    '01','02','04','05','06','08','09','10','11','12','13','15','16','17','18',
    '19','20','21','22','23','24','25','26','27','28','29','30','31','32','33',
    '34','35','36','37','38','39','40','41','42','44','45','46','47','48','49',
    '50','51','53','54','55','56'
    }


bea_url = (
    f"https://apps.bea.gov/api/data"
    f"?UserID={bea_key}"
    f"&method=GetData"
    f"&datasetname=Regional"
    f"&TableName=SAGDP2"
    f"&LineCode=1"
    f"&GeoFips=STATE"
    f"&Year=ALL"
    f"&ResultFormat=JSON"
)

if is_fetched("bea"):
    print("bea_gdp already fetched — skipping")
else:
    # fetch
    try:
        response = requests.get(bea_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"ERROR fetching BEA data: {e}")
        log_fetch("bea", "error")
        exit()

    # parse
    try:
        data = response.json()
        records = data["BEAAPI"]["Results"]["Data"]
    except Exception as e:
        print(f"ERROR parsing BEA response: {e}")
        log_fetch("bea", "error")
        exit()

    # filter & clean
    rows_bea = []
    for r in records:
        fips_prefix = r["GeoFips"][:2]
        year = int(r["TimePeriod"])
        if fips_prefix in state_fips and 2000 <= year <= 2023:
            rows_bea.append(
                {
                    "state": r["GeoName"],
                    "fips": r["GeoFips"],
                    "year": year,
                    "gdp_millions": r["DataValue"].replace(",", ""),
                }
            )

    if not rows_bea:
        print("No BEA data returned")
        log_fetch("bea", "no_data")
        exit()

    # build dataframe
    bea_gdp = pd.DataFrame(rows_bea)
    bea_gdp["gdp_millions"] = pd.to_numeric(bea_gdp["gdp_millions"], errors="coerce")

    print(bea_gdp.shape)
    print(bea_gdp.head())

    # save
    bea_gdp.to_sql("bea_gdp", engine, if_exists="replace", index=False)
    log_fetch("bea", "success")
    print("bea_gdp saved!")
