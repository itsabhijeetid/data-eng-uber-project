# Databricks notebook source
# MAGIC %md
# MAGIC ### **Intial load all the file from ADLS**

# COMMAND ----------

import pandas as pd
from urllib.parse import quote

files = [
{"file": "map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:

    url = f"https://dldevabhi.blob.core.windows.net/raw/Uber%20Project/Ingestion/{file['file']}.json?<token>"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
    
    # Writing Data To the Bronze Layer
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

# MAGIC %md
# MAGIC > ### **Intial Load History Table**

# COMMAND ----------

url = "https://dldevabhi.blob.core.windows.net/raw/Uber%20Project/Ingestion/bulk_rides.json?<token>"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")