-- Databricks notebook source
SELECT COUNT(*) FROM dev_traffic.bronze_db.raw_traffic

-- COMMAND ----------

SELECT COUNT(*) FROM dev_traffic.silver_db.traffic_data

-- COMMAND ----------

SELECT COUNT(*) FROM dev_traffic.gold_db.traffic_data