from db import engine, table_summary
from sqlalchemy import text
import requests

def check_url(label, url):
    try:
        response = requests.get(url, timeout=10)
        print(f"  {'OK' if response.status_code == 200 else 'FAILED'} ({response.status_code}): {label}")
    except Exception as e:
        print(f"  ERROR: {label} — {e}")

# API connections
print("=== API CONNECTIONS ===")
check_url("BEA", "https://apps.bea.gov/api/data")
check_url("EIA", "https://api.eia.gov/v2")
check_url("NOAA", "https://www.ncei.noaa.gov/cdo-web/api/v2/data")
check_url("NREL","https://developer.nrel.gov/api/nsrdb/v2")
check_url("PVGIS","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc")
check_url("OWID","https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv")
check_url("CENSUS", "https://www2.census.gov/programs-surveys/popest/")

# table summaries
print("\nTABLE SUMMARIES")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """))
    db_tables = [row[0] for row in result]

for table in db_tables:
    table_summary(table)