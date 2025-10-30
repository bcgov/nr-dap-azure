# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%sparkr
# MAGIC install.packages("bcdata")
# MAGIC library(tidyverse)
# MAGIC library(dplyr)
# MAGIC library(bcdata)
# MAGIC 
# MAGIC # ID dfc492c0-69c5-4c20-a6de-2c9bc999301f = natural-resource-nr-regions
# MAGIC nr_region <- bcdc_get_data("dfc492c0-69c5-4c20-a6de-2c9bc999301f") %>%
# MAGIC   rename("NR_REGION" = "REGION_NAME") %>%
# MAGIC   select(c("NR_REGION", "geometry"))
# MAGIC 
# MAGIC nr_region$NR_REGION <- str_replace(nr_region$NR_REGION , "Natural Resource Region", "")
# MAGIC 
# MAGIC # ID 0bc73892-e41f-41d0-8d8e-828c16139337 = natural-resource-nr-district
# MAGIC nr_district <- bcdc_get_data("0bc73892-e41f-41d0-8d8e-828c16139337") %>%
# MAGIC   rename("NR_DISTRICT" = "DISTRICT_NAME") %>%
# MAGIC   select(c("NR_DISTRICT", "geometry"))
# MAGIC 
# MAGIC nr_district$NR_DISTRICT  <-  str_replace(nr_district$NR_DISTRICT, "Natural Resource District", "")
# MAGIC 
# MAGIC # ID 0bc73892-e41f-41d0-8d8e-828c16139337 = ? 
# MAGIC geo_data <- bcdc_get_data("43805524-4add-4474-ad53-1a985930f352") %>% 
# MAGIC   select(c("LATITUDE", "LONGITUDE")) %>%
# MAGIC   group_by_all() %>%
# MAGIC   slice(1)
# MAGIC 
# MAGIC display(nr_region)
# MAGIC display(nr_district)
# MAGIC display(geo_data)

# METADATA ********************

# META {
# META   "language": "r",
# META   "language_group": "synapse_pyspark"
# META }
