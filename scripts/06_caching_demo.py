# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Caching Demo
# MAGIC
# MAGIC 1. Disk cache (otomatis bila instance type SSD).
# MAGIC 2. Bandingkan cold run vs warm run.
# MAGIC 3. Hindari `.cache()` / `.persist()` untuk Delta.
# MAGIC
# MAGIC Docs: https://learn.microsoft.com/azure/databricks/optimizations/disk-cache

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import time

QUERY = f"""
    SELECT country, product_id, sum(total_amount) revenue
    FROM   {FQN('sales_clustered')}
    WHERE  order_date BETWEEN date '2024-01-01' AND date '2024-03-31'
    GROUP BY country, product_id
"""

def run(label):
    t0 = time.time()
    n = spark.sql(QUERY).count()
    print(f"{label:8s}  rows={n:,}  elapsed={time.time()-t0:.2f}s")

# COMMAND ----------
# Cek apakah disk cache aktif:
print("disk cache enabled?",
      spark.conf.get("spark.databricks.io.cache.enabled", "default"))

# COMMAND ----------
run("COLD")   # query pertama → harus baca dari ABFSS
run("WARM-1") # query kedua  → harusnya jauh lebih cepat (hit disk cache)
run("WARM-2")

# COMMAND ----------
# MAGIC %md ## ❌ Anti-pattern: Spark cache pada Delta
df = spark.table(FQN("sales_clustered")).cache()
df.count()       # materialise
df.filter("country='ID'").count()
# Risiko:
#  - kehilangan data skipping pada filter tambahan
#  - data stale kalau tabel di-update via path lain
df.unpersist()

# COMMAND ----------
# MAGIC %md ## ✅ Pre-warm cache untuk dashboard pagi hari
# Jalankan via Databricks Workflow tiap pagi sebelum jam kerja.
spark.sql(f"SELECT count(*) FROM {FQN('sales_clustered')} WHERE order_date >= current_date() - INTERVAL 30 DAYS").show()
