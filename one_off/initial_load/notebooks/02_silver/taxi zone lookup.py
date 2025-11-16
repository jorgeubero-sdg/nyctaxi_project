# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

# Read the taxi zone lookup CSV (with header) into a DataFrame
df = spark.read.format("csv").option("header", True)\
.load("/Volumes/az_core_realstate_internal_training_catalog/00_landing_jrg/data_sources/lookup/taxi_zone_lookup.csv")


# COMMAND ----------

df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
            )


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("az_core_realstate_internal_training_catalog.02_silver_jrg.taxi_zone_lookup")

# COMMAND ----------

df.display()