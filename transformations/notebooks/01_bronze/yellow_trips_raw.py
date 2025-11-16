# Databricks notebook source
from datetime import date
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(f"/Volumes/az_core_realstate_internal_training_catalog/00_landing_jrg/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# Add a column to capture when the data was processed
df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("az_core_realstate_internal_training_catalog.01_bronze_jrg.yellow_trips_raw")