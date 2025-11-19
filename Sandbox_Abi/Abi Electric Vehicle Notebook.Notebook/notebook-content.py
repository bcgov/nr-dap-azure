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

# 1. Extract 
df = spark.sql("SELECT * FROM DAP_2_Proof_of_Concept_Lakehouse.ElectricVehicleData LIMIT 300")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Explore
print(df[['ModelYear', 'ElectricRange', 'BaseMSRP', 'ElectricVehicleType']].describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Transform
from pyspark.sql.functions import when, col
df = df.withColumn(
    "ElectricVehicleType",
    when(col("ElectricVehicleType") == "Battery Electric Vehicle (BEV)", "BEV")
    .when(col("ElectricVehicleType") == "Plug-in Hybrid Electric Vehicle (PHEV)", "PHEV")
    .otherwise(col("ElectricVehicleType"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Load as a Dimensional Model
fact_vehicle = df[['VIN', 'Make', 'Model']]
dim_location = df[['VIN', 'County', 'City', 'PostalCode', 'State']]
dim_type = df[['VIN', 'ElectricVehicleType', 'ElectricRange', 'BaseMSRP']]
display(dim_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
