# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Photon Benchmark
# MAGIC
# MAGIC Tujuan: rasakan perbedaan latency Photon ON vs OFF pada workload analitik.
# MAGIC
# MAGIC Cara pakai:
# MAGIC 1. Buat 2 cluster identik — satu **Photon ON**, satu **OFF**.
# MAGIC 2. Attach notebook ini ke masing-masing, jalankan, catat waktu.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import time

# Cek runtime engine — Photon biasanya terlihat dari clusterUsageTags.
# Cara paling akurat: cek halaman Compute → field "Runtime" mengandung "Photon".
for key in [
    "spark.databricks.clusterUsageTags.runtimeEngine",
    "spark.databricks.photon.enabled",
]:
    print(f"{key} = {spark.conf.get(key, 'n/a')}")

QUERIES = {
    "Q1 - aggregate":  f"""
        SELECT country, sum(total_amount) FROM {FQN('sales_clustered')}
        GROUP BY country
    """,
    "Q2 - join+agg":   f"""
        SELECT p.category, count(*) cnt, sum(s.total_amount) rev
        FROM   {FQN('sales_clustered')} s
        JOIN   {FQN('products')}        p USING (product_id)
        GROUP BY p.category
    """,
    "Q3 - window":     f"""
        SELECT customer_id,
               sum(total_amount)  OVER (PARTITION BY customer_id) total_spent,
               row_number()       OVER (PARTITION BY customer_id ORDER BY order_ts DESC) rn
        FROM {FQN('sales_clustered')}
        WHERE order_date >= current_date() - INTERVAL 90 DAYS
        LIMIT 1000
    """,
}

for name, q in QUERIES.items():
    t0 = time.time()
    spark.sql(q).count()
    print(f"{name:18s}  {time.time()-t0:6.2f}s")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Tips Cluster Sizing (resmi Databricks)
# MAGIC | Workload | Saran |
# MAGIC |----------|-------|
# MAGIC | ETL batch  | Larger cluster, autoscale rendah-tinggi, Photon ON |
# MAGIC | SQL BI     | SQL Warehouse Serverless (Photon default) |
# MAGIC | Streaming  | Job cluster dedicated, sesuai max ingest rate |
# MAGIC | Ad-hoc     | All-purpose dengan auto-termination 30-60 min |
