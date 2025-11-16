# Databricks notebook source
# MAGIC %md
# MAGIC ## which vendor makes the most revenue?

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.table("az_core_realstate_internal_training_catalog.02_silver_jrg.yellow_trips_enriched")

df.\
    groupBy("vendor").\
    agg(
        round(sum("total_amount"),2).alias("total_revenue")
        ).\
    orderBy("total_revenue",ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## which is the most popular pickup borough?

# COMMAND ----------

df.\
    groupBy("pu_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## most common journey

# COMMAND ----------

df.\
    groupBy(concat("pu_borough", lit(" -> "), "do_borough").alias("journey")).\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##timne series with the number of trips and total revenue per day

# COMMAND ----------

df2 = spark.read.table("az_core_realstate_internal_training_catalog.03_gold_jrg.daily_trip_summary")

df2.display()