# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Read all Parquet files from the landing directory into a DataFrame
df = spark.read.format("parquet").load("/Volumes/az_core_realstate_internal_training_catalog/00_landing_jrg/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

# Add a column to capture when the data was processed
df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, overwriting any existing data
df.write.mode("overwrite").saveAsTable("az_core_realstate_internal_training_catalog.01_bronze_jrg.yellow_trips_raw")

# COMMAND ----------

df.limit(25).display()