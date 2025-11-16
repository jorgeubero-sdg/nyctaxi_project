# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

spark.read.table("az_core_realstate_internal_training_catalog.`01_bronze_jrg`.yellow_trips_raw").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("az_core_realstate_internal_training_catalog.`02_silver_jrg`.yellow_trips_cleansed").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("az_core_realstate_internal_training_catalog.`02_silver_jrg`.yellow_trips_enriched").\
    groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("az_core_realstate_internal_training_catalog.`03_gold_jrg`.daily_trip_summary").\
    groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month")).\
    agg(sum("total_trips").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("az_core_realstate_internal_training_catalog.`02_silver_jrg`.taxi_zone_lookup").display()