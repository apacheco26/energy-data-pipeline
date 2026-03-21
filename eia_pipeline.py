import requests
import pandas as pd
import time
import os
from sqlalchemy import text
from db import engine, check_data_exists, is_fetched, log_fetch

# for API help --> https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf 
api_key = os.environ["EIA_API_KEY"]


# edit to send to table in chunks 
# data was getting too large to store in memory
def fetch_eia_data(url, label, table_name, mode="replace", state = "ALL"):
    """Fetch and save in chunks to avoid memory issues"""
    
    # limit tries
    max_retries = 3
    retries = 0    
    offset = 0
    length = 5000
    first_chunk = True

    while True:
        count_url = f"{url}&offset={offset}&length={length}"
        # cnnect to url
        response = requests.get(count_url)

        if not response.text.strip():
            retries  += 1
            if retries >= max_retries:
                log_fetch(label, "error", state=state)
                return
        
            time.sleep(2)
            continue
        try:
            data = response.json()

        except Exception as e:
            retries += 1
            if retries >= max_retries:
                log_fetch(label, "error", state=state)
                return
            time.sleep(2)
            continue

        # ran into issues before 
        #fixed them...just in case
        if "error" in data:
            print(f"Error on {label}: {data['error']}")
            log_fetch(label, "error", state=state)
            break
        
        rows = data["response"]["data"]
        total = int(data["response"]["total"])

        # save chunk directly to Postgres
        df_chunk = pd.DataFrame(rows)

        # need to avg capacity in python
        # the fetching and saving to table is too large to do in memory
        if table_name == "eia_capacity":
            df_chunk["year"] = df_chunk["period"].str[:4]
            df_chunk["nameplate-capacity-mw"] = pd.to_numeric(df_chunk["nameplate-capacity-mw"], errors="coerce").fillna(0)
            df_chunk = df_chunk.groupby(
                ["year", "stateid", "stateName", "technology", "energy_source_code"],
                as_index=False
            )["nameplate-capacity-mw"].sum()

        # when 5000 hit change to append 
        # save after 
        in_or_out = mode if first_chunk else "append"

        # makes use of mode
        df_chunk.to_sql(table_name, engine, if_exists=in_or_out, index=False)
        first_chunk = False

        offset += len(rows)
        print(f"{label} - saved {offset} of {total} rows")

        if offset >= total:
            log_fetch(label , "success", state=state)
            break

        time.sleep(0.5)

    print(f"{table_name} saved! ")
    
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

# added a if not statement to help with data being recreated after each issue

print("Electricity produced by fuel type per year....")
if not is_fetched("eia_generation"):
    fetch_eia_data(gen_url, "Generation", "eia_generation")
else:
    print("eia table already created")

print("Retail sales")
if not is_fetched("eia_sales"):
    fetch_eia_data(sales_url, "Retail Sales", "eia_sales")
else:
    print("Retail sales table already created")

print("Total Energy...")
if not is_fetched("eia_total_energy"):
    fetch_eia_data(total_url, "Total Energy", "eia_total_energy")
else:
    print("total energy table already created")

print("CO2 Emissions...")
if not is_fetched("eia_co2_emissions"):
    fetch_eia_data(co2_url, "CO2 Emissions", "eia_co2_emissions")
else:
    print("co2 emission table already created")

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
        "facet_prefix": ""
    },
    {
        "label": "NaturalGas",
        "url": "https://api.eia.gov/v2/natural-gas/cons/sum/data",
        "data_col": "value",
        "facet": "duoarea",
        "facet_prefix": "S"
    },
    {
        "label": "Coal",
        "url": "https://api.eia.gov/v2/coal/consumption-and-quality/data",
        "data_col": "consumption",
        "facet": "location",
        "facet_prefix": ""
    }
    ]

for dataset in url_info:
    print(f"\n{'='*50}")
    print(f"Fetching {dataset['label']}...")
    print(f"{'='*50}")

    table_name = dataset["label"].lower()

    for i, state in enumerate(states):
        if is_fetched(dataset["label"], state=state):
            print(f"Skipping {dataset['label']} {state} because already fetched")
            continue
        mode = "replace" if i == 0 else "append"
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
        fetch_eia_data(url, f"{dataset['label']}-{state}", table_name, mode = mode, state=state)
    
print("capacity by fuel type per year")
if not is_fetched("eia_capacity"):
    fetch_eia_data(cap_url, "Capacity", "eia_capacity")
else:
    print("capacity table already created")

