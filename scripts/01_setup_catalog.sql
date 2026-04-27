-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Setup Catalog, Schema, Volume (SQL version)
-- MAGIC Versi SQL dari `00_config.py`. Jalankan sekali di SQL Editor / SQL Warehouse.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS learn_optimize;
CREATE SCHEMA  IF NOT EXISTS learn_optimize.tutorial;
CREATE VOLUME  IF NOT EXISTS learn_optimize.tutorial.raw;

-- COMMAND ----------
-- Aktifkan Predictive Optimization (rekomendasi resmi Databricks).
-- Butuh role MANAGE pada catalog. Lewati kalau workspace kamu belum support.

ALTER CATALOG learn_optimize ENABLE PREDICTIVE OPTIMIZATION;

-- COMMAND ----------

SHOW SCHEMAS IN learn_optimize;
