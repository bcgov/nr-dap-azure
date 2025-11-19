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

# Warm-up cell to initialize Spark session
spark.range(1).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Some PySpark code to execute to test notebook

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session (usually already available in Fabric notebooks)
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Jan", 200), ("Feb", 220), ("Mar", 250), ("Apr", 275), ("May", 300)]
columns = ["Month", "Sales"]

# Create a Spark DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Add a new column with 10% increase in sales
df_with_growth = df.withColumn("Sales_Next_Month", col("Sales") * 1.10)

# Show the updated DataFrame
df_with_growth.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.range(1, 5).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the table (assuming it's registered in the Lakehouse)
df = spark.read.table("ElectricVehicleData")

# Select relevant columns
selected_df = df.select(
    "ModelYear",
    "Make",
    "Model",
    "ElectricVehicleType",
    "ElectricRange"
)

# Show a sample of the data
selected_df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, first

# Load the table
df = spark.read.table("ElectricVehicleData")

# Group and aggregate
grouped_df = df.groupBy("ModelYear", "Make", "Model").agg(
    count("*").alias("ModelCount"),
    first("ElectricVehicleType").alias("ElectricVehicleType"),
    first("ElectricRange").alias("ElectricRange")
)

# Show the result
grouped_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
