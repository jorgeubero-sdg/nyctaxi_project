# Databricks notebook source
# Creating the schemas
spark.sql("create schema if not exists az_core_realstate_internal_training_catalog.00_landing_jrg")
spark.sql("create schema if not exists az_core_realstate_internal_training_catalog.01_bronze_jrg")
spark.sql("create schema if not exists az_core_realstate_internal_training_catalog.02_silver_jrg")
spark.sql("create schema if not exists az_core_realstate_internal_training_catalog.03_gold_jrg")

# COMMAND ----------

# Creating the volume
spark.sql("create volume if not exists az_core_realstate_internal_training_catalog.00_landing_jrg.data_sources")