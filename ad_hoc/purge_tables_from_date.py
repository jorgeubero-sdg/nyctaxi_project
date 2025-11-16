# Databricks notebook source
# Update this so that the date is the start of the month that was 2 months prior to the current date
date_from = '2025-09-01'

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "az_core_realstate_internal_training_catalog.`01_bronze_jrg`.yellow_trips_raw")

dt.delete(f"tpep_pickup_datetime >= {date_from}")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "az_core_realstate_internal_training_catalog.`02_silver_jrg`.yellow_trips_cleansed")

dt.delete(f"tpep_pickup_datetime >= {date_from}")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "az_core_realstate_internal_training_catalog.`02_silver_jrg`.yellow_trips_enriched")

dt.delete(f"tpep_pickup_datetime >= {date_from}")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "az_core_realstate_internal_training_catalog.`03_gold_jrg`.daily_trip_summary")

dt.delete(f"pickup_date >= {date_from}")