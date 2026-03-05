import requests
import pandas as pd
import json
import time
import io
import os
import api_config


# # U.S Energy Information Administration

# for API help --> https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf 
api_key = api_config.api_key


def fetch_eia_data(url, label):
    """Function to collect all values max on EIA website it 5000"""

    all_data = []
    offset = 0

    # website unique fetch length
    length = 5000

    while True:
        count_url=f"{url}&offset={offset}&length={length}"
        response=requests.get(count_url)


        try:
            data=response.json()
        except Exception as e:
            print(f"JSON error on {label} at offset {offset}: {e}")
            print(f"Raw response: {response.text[:200]}")  # peek at what came back
            break


        # if error
        if "error" in data:
            print(f"Error on {label}: {data['error']}")
            break

        rows = data["response"]["data"]
        total = int(data["response"]["total"])

        all_data.extend(rows)
        print(f"{label} - fetched {len(all_data)} of {total} rows")

        # once we get everything
        if len(all_data) >= total:
            break

        offset += length

        time.sleep(0.5)

    return pd.DataFrame(all_data)

# Electricity produced by fuel type per year
gen_url = (
    f"https://api.eia.gov/v2/electricity/electric-power-operational-data/data"
    f"?api_key={api_key}"
    f"&data[]=generation"
    f"&frequency=annual"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )


# installed capacity by fuel type per year
cap_url = (
    f"https://api.eia.gov/v2/electricity/operating-generator-capacity/data"
    f"?api_key={api_key}"
    f"&data[]=nameplate-capacity-mw"
    f"&frequency=monthly"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )

# price,revenue,sales, customers per year
sales_url = (
    f"https://api.eia.gov/v2/electricity/retail-sales/data"
    f"?api_key={api_key}"
    f"&data[]=revenue&data[]=sales&data[]=price&data[]=customers"
    f"&frequency=annual"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )

# Total Energy
total_url = (
    f"https://api.eia.gov/v2/total-energy/data"
    f"?api_key={api_key}"
    f"&data[]=value"
    f"&frequency=annual"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )

# CO2 Emissions
co2_url = (
    f"https://api.eia.gov/v2/co2-emissions/co2-emissions-aggregates/data"
    f"?api_key={api_key}"
    f"&data[]=value"
    f"&frequency=annual"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )


print("Electricity produced by fuel type per year....")
df_generation = fetch_eia_data(gen_url, "Generation")
print(f"Generation rows : {len(df_generation)}")
df_generation.to_csv("eia_generation.csv", index=False)


print("capacity by fuel type per year")
df_capacity = fetch_eia_data(cap_url, "Capacity")
print(f"Capacity rows: {len(df_capacity)}")
df_capacity.to_csv("eia_capacity.csv", index=False)


print("Retail sales")
df_sales = fetch_eia_data(sales_url, "Retail Sales")
print(f"Sales rows: {len(df_sales)}")
df_sales.to_csv("eia_sales.csv", index=False)


print("Total Energy...")
df_total = fetch_eia_data(total_url, "Total Energy")
print(f"Saved staging_eia_total_energy.csv ({len(df_total)} rows)")
df_total.to_csv("eia_total_energy.csv", index=False)


print("CO2 Emissions...")
df_co2 = fetch_eia_data(co2_url, "CO2 Emissions")
print(f"Saved staging_eia_co2_emissions.csv ({len(df_co2)} rows)")
df_co2.to_csv("eia_co2_emissions.csv", index=False)


# # EIA but fetch required different setup 


states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
          ]

url_info = [
        {
        "label": "SEDS",
        "url": "https://api.eia.gov/v2/seds/data",
        "data_col": "value",
        "facet": "stateId",
        "facet_prefix": "",
        "output": "eia_seds.csv"},
        {
        "label": "NaturalGas",
        "url": "https://api.eia.gov/v2/natural-gas/cons/sum/data",
        "data_col": "value",
        "facet": "duoarea",
        "facet_prefix": "S",
        "output": "eia_natural_gas.csv"},
        {
        "label": "Coal",
        "url": "https://api.eia.gov/v2/coal/consumption-and-quality/data",
        "data_col": "consumption",
        "facet": "location",
        "facet_prefix": "",
        "output": "eia_coal.csv"}
        ]

for dataset in url_info:
    print(f"\n{'='*50}")
    print(f"Fetching {dataset['label']}...")
    print(f"{'='*50}")

    all_rows = []

    for state in states:
        facet_value = dataset["facet_prefix"] + state
        print(f"{dataset['label']} — {state}")

        url = (
            f"{dataset['url']}"
            f"?api_key={api_key}"
            f"&data[]={dataset['data_col']}"
            f"&facets[{dataset['facet']}][]={facet_value}"
            f"&frequency=annual"
            f"&start=2000"
            f"&end=2023"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=asc"
            )

        df_state = fetch_eia_data(url, f"{dataset['label']}-{state}")
        all_rows.append(df_state)

    df_final = pd.concat(all_rows, ignore_index=True)
    df_final.to_csv(dataset["output"], index=False)
    print(f"{dataset['label']} saved — {len(df_final)} rows = {dataset['output']}")



# # U.S Energy Information Administration

# International Energy
intl_url = (
    f"https://api.eia.gov/v2/international/data"
    f"?api_key={api_key}"
    f"&data[]=value"
    f"&frequency=annual"
    f"&start=2000"
    f"&end=2023"
    f"&sort[0][column]=period"
    f"&sort[0][direction]=asc"
    )

print("International Energy...")
df_intl = fetch_eia_data(intl_url, "International Energy")
print(f" Saved staging_eia_international.csv ({len(df_intl)} rows)")


df_intl.to_csv("eia_international.csv", index=False)


# # NOAA

# API Documentation --> https://developer.nrel.gov

# Personal note, this API key goes in URL similar to EIA

api_key_NREL = api_config.api_key_NREL

# state coord to work with API
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

nrel_rows = []

# for loop to group by state during each fetch
for state, (lat,lon) in states_coords.items():
    print(f"fetching data for {state}...")

    url_NREL = (
        f"https://developer.nrel.gov/api/solar/solar_resource/v1.json"
        f"?api_key={api_key_NREL}"
        f"&lat={lat}"
        f"&lon={lon}"
    )

    response = requests.get(url_NREL)

    # try to avoid errors that occcured for some states
    try:
        data = response.json()
    except:
        print(f"Error fetching {state} json request")
        continue

    if data["errors"]:
        print(f"error for {state}: {data['errors']}")
        continue

    outputs = data["outputs"]

    # Error with Alaska skip and come back later
    if not isinstance(outputs.get("avg_ghi"), dict):
        print(f" Skipping {state} — no data available")
        continue

    if not isinstance(outputs.get("avg_dni"), dict):
        print(f" Skipping {state} — no data available")
        continue

    if not isinstance(outputs.get("avg_lat_tilt"), dict):
        print(f" Skipping {state} — no data available")
        continue

    # join all states
    nrel_rows.append({
        "state": state,
        "lat": lat,
        "lon": lon,

        # solar potential 
        "avg_ghi_annual": outputs["avg_ghi"]["annual"],

        # direct sunlight
        "avg_dni_annual": outputs["avg_dni"]["annual"],

        # panel tilt
        "avg_lat_tilt": outputs["avg_lat_tilt"]["annual"]
        })

    time.sleep(0.5)

# organize in dataframe
df_nrel = pd.DataFrame(nrel_rows)

# export to csv
df_nrel.to_csv("nrel_solar.csv", index=False)
print("DONE!!")
print(df_nrel.head(25))


# Preliminary overview of the data from NREL looks good.
# 
# Note, no data for Alaska in both NOAA and NREL. 

# # NSRB National Solar Radiation Database

email_key = api_config.email_key

nsrdb_rows = []

# If errors 
completed_states = []

for state, (lat, lon) in states_coords.items():
    if state == "AK" or state in completed_states:
        continue
    for year in range(2000, 2024):
        print(f"  {state} — {year}")
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
        try:
            response = requests.get(url)
            lines = response.text.split("\n")
            data_start = next(i for i, l in enumerate(lines) if l.startswith("Year"))
            df = pd.read_csv(io.StringIO("\n".join(lines[data_start:])))
            nsrdb_rows.append({
                "state": state,
                "year": year,
                "avg_ghi": round(df["GHI"].mean(), 2),
                "avg_dni": round(df["DNI"].mean(), 2)
            })
        except Exception as e:
            print(f"  ERROR {state} {year}: {e}")
        time.sleep(1)

df_nsrdb = pd.DataFrame(nsrdb_rows)
df_nsrdb.to_csv("nsrdb_solar_annual.csv", index=False)
print(f"Done! {df_nsrdb.shape}")


# Bureau of Economic Analysis


# check connection of API 
bea_key = api_config.bea_key

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

bea_gdp.to_csv("bea_gdp.csv", index=False)
print("Saved!")

target_countries = [
    "China", "Germany", "India", "Brazil",
    "United Kingdom", "Australia", "France", "Japan", "Denmark",
    "Mexico", "Canada"
    ]

df_owid = pd.read_csv("owid-energy-data.csv")
found = df_owid[df_owid["country"].isin(target_countries)]["country"].unique()
missing = set(target_countries) - set(found)

print(f"Found: {found}")
print(f"Missing: {missing}")

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
df_pvgis.to_csv("pvgis_solar_raw.csv", index=False)
print(f"Done! {df_pvgis.shape}")


# # U.S State Population

state_pop_key = api_config.state_pop_key

raw = json.loads(response.text)
header = raw[0]
rows_2010s = []
for row in raw[1:]:
    record = dict(zip(header, row))
    if "7/1/" in record["DATE_DESC"] and "population estimate" in record["DATE_DESC"]:
        year = int(record["DATE_DESC"].split("/")[2].split(" ")[0])
        rows_2010s.append({
            "state": record["NAME"],
            "fips": record["state"].zfill(2),
            "year": year,
            "population": int(record["POP"])
            })

# Parse 2000s
raw2 = json.loads(response2.text)
header2 = raw2[0]
rows_2000s = []
for row in raw2[1:]:
    record = dict(zip(header2, row))
    if "7/1/" in record["DATE_DESC"] and "population estimate" in record["DATE_DESC"]:
        year = int(record["DATE_DESC"].split("/")[2].split(" ")[0])
        if 2000 <= year <= 2009:
            rows_2000s.append({
                "fips": record["state"].zfill(2),
                "year": year,
                "population": int(record["POP"])
                })

# state name mapping from 2010s data
fips_to_state = {row["fips"]: row["state"] for row in rows_2010s}

# Add state names to 2000s rows
for row in rows_2000s:
    row["state"] = fips_to_state.get(row["fips"], None)

print(len(rows_2010s), len(rows_2000s))
print(rows_2000s[:3])

import io

url3 = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2023/state/totals/NST-EST2023-ALLDATA.csv"
)

response3 = requests.get(url3)
print(response3.status_code)
print(response3.text[:500])

df3 = pd.read_csv(io.StringIO(response3.text))

# Filter to states only (SUMLEV == 40)
df3 = df3[df3["SUMLEV"] == 40]

rows_2020s = []
for _, row in df3.iterrows():
    for year in [2020, 2021, 2022, 2023]:
        rows_2020s.append({
            "state": row["NAME"],
            "fips": str(row["STATE"]).zfill(2),
            "year": year,
            "population": int(row[f"POPESTIMATE{year}"])
        })

print(len(rows_2020s))
print(rows_2020s[:3])

all_rows = rows_2000s + rows_2010s + rows_2020s

census_pop = pd.DataFrame(all_rows)
census_pop = census_pop[["state", "fips", "year", "population"]]
census_pop = census_pop.sort_values(["state", "year"]).reset_index(drop=True)

print(census_pop.shape)
print(census_pop.head())

census_pop.to_csv("census_population.csv", index=False)
print("Saved!")

csv_files = ["bea_gdp.csv", "census_population.csv", "eia_capacity.csv","eia_co2_emissions.csv", 
             "eia_coal.csv", "eia_generation.csv","eia_international.csv", "eia_natural_gas.csv", 
             "eia_sales.csv", "eia_seds.csv", "eia_total_energy.csv","noaa_climate.csv","nrel_solar.csv",
             "owid-energy-data.csv", "nsrdb_solar_annual.csv"]


for file in csv_files:
    size = os.path.getsize(file) if os.path.exists(file) else "NOT FOUND"
    num_row = pd.read_csv(file)

    print(f"{file}: {size} bytes")
    print(f"{file}: {num_row.shape[0]} rows\n")


pd.read_csv("nsrdb_solar_annual.csv").head()

