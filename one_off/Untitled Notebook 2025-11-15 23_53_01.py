# Databricks notebook source
from pyspark.sql.functions import *

spark.read.table("az_core_realstate_internal_training_catalog.02_silver_jrg.yellow_trips_cleansed").\
    agg(max("tpep_pickup_datetime").alias("max_tpep_pickup_datetime"),min("tpep_pickup_datetime").alias("min_tpep_pickup_datetime")).\
    display()