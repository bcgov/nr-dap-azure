# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5fc7437d-cf11-4367-8ad2-db82d69028a7",
# META       "default_lakehouse_name": "DAP_2_Proof_of_Concept_Lakehouse",
# META       "default_lakehouse_workspace_id": "0600aa7d-b62b-4b56-bddc-7271d006079c",
# META       "known_lakehouses": [
# META         {
# META           "id": "5fc7437d-cf11-4367-8ad2-db82d69028a7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from bs4 import BeautifulSoup
import csv
import requests

url = "https://api.statcan.gc.ca/census-recensement/profile/sdmx/rest/data/STC_CP,DF_CMACA/A5.2021S0504501.../statsCanDataPull.xml"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"failed with {response.status_code}")

soup = BeautifulSoup(response.text, "xml")

ns = {
    "generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
}

# Extract all series
series_list = soup.find_all("generic:Series")

data = []

for series in series_list:
    # Extract SeriesKey values
    series_key = {v["id"]: v["value"] for v in series.find("generic:SeriesKey").find_all("generic:Value")}
    
    # Extract observation
    obs = series.find("generic:Obs")
    try:
        obs_dim = obs.find("generic:ObsDimension")["id"]
    except:
        obs_dim = None

    try:
        obs_val = obs.find("generic:ObsDimension")["value"]
    except:
        obs_val = None
    
    # Combine all into one row
    row = {**series_key, "TIME_PERIOD": obs_dim, "VALUE": obs_val}
    data.append(row)


import pandas as pd
df = pd.DataFrame(data)

# df.to_csv("StatsCan_CA-Census_2021.csv", index=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.to_csv("/lakehouse/default/Files/StatsCan_CA-Census_2021.csv", index=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.to_csv("/lakehouse/default/Files/census_2021/StatsCan_CA-Census_2021.csv", index=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

! ls -ltr /lakehouse/default/Files/census_2021/

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

! pwd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
