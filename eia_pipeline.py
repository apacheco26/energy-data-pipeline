import requests
import pandas as pd
import time
import os
from db import engine

# for API help --> https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf 
api_key = os.environ["EIA_API_KEY"]


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
            print(f"Raw response: {response.text[:200]}")
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

df_generation.to_sql("eia_generation", engine, if_exists="replace", index=False)

print("capacity by fuel type per year")
df_capacity = fetch_eia_data(cap_url, "Capacity")
print(f"Capacity rows: {len(df_capacity)}")
df_capacity.to_sql("eia_capacity", engine, if_exists="replace", index=False)

print("Retail sales")
df_sales = fetch_eia_data(sales_url, "Retail Sales")
print(f"Sale rows: {len(df_sales)}")
df_sales.to_sql("eia_sales", engine, if_exists="replace", index=False)

print("Total Energy...")
df_total = fetch_eia_data(total_url, "Total Energy")
print(f"({len(df_total)} rows)")
df_total.to_sql("eia_total_energy", engine, if_exists="replace", index=False)

print("CO2 Emissions...")
df_co2 = fetch_eia_data(co2_url, "CO2 Emissions")
print(f"(co2 {len(df_co2)} rows)")
df_co2.to_sql("eia_co2_emissions", engine, if_exists="replace", index=False)

# fetch required different setup for below

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