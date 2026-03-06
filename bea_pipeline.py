import requests
import pandas as pd
import os
from db import engine, save_to_jsonb

# check connection of API 
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


response = requests.get(bea_url)
data = response.json()

records = data["BEAAPI"]["Results"]["Data"]

rows_boa= []
for r in records:
    fips_prefix = r["GeoFips"][:2]
    year = int(r["TimePeriod"])
    if fips_prefix in state_fips and 2000 <= year <= 2023:
        rows_boa.append({
            "state": r["GeoName"],
            "fips": r["GeoFips"],
            "year": year,
            "gdp_millions": r["DataValue"]
            })

bea_gdp = pd.DataFrame(rows_boa)
print(bea_gdp.shape)
print(bea_gdp.head())

save_to_jsonb(bea_gdp,"bea_gdp", engine)
print("Saved!")