import bea_pipeline
import census_pipeline
import owid_pipeline
import eia_pipeline
import nsrdb_pipeline
import noaa_pipeline
import pvgis_pipeline
import nrel_pipeline
from db import init_fetch_log

if __name__ == "__main__":
    print("=== Energy Data Pipeline ===\n")

    print("Initializing fetch log...")
    init_fetch_log()

    print("\n--- 1. BEA: GDP per Capita ---")
    bea_pipeline.run()

    print("\n--- 2. Census: State Population ---")
    census_pipeline.run()

    print("\n--- 3. OWID: International Energy ---")
    owid_pipeline.run()

    print("\n--- 4. EIA: Generation, Sales, CO2, Capacity ---")
    eia_pipeline.run()

    print("\n--- 5. NSRDB: Solar Radiation by State ---")
    nsrdb_pipeline.run()

    print("\n--- 6. NOAA: Climate Data by State ---")
    noaa_pipeline.run()

    print("\n--- 7. PVGIS: International Solar Radiation ---")
    pvgis_pipeline.run()

    print("\n--- 8. NREL: Solar Resource Summary ---")
    nrel_pipeline.run()

    print("\n=== All pipelines complete ===")
