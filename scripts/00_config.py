# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Konfigurasi Global
# MAGIC
# MAGIC Notebook ini mendefinisikan **catalog**, **schema**, dan **volume** yang dipakai
# MAGIC oleh seluruh tutorial. Jalankan **sekali** saja, lalu di setiap notebook lain
# MAGIC kamu bisa `%run ./00_config` untuk meng-import variabel.

# COMMAND ----------

CATALOG = "learn_optimize"
SCHEMA  = "tutorial"
VOLUME  = "raw"   # Unity Catalog Managed Volume untuk file landing (Auto Loader)

FQN = lambda table: f"`{CATALOG}`.`{SCHEMA}`.`{table}`"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"✅ Catalog : {CATALOG}")
print(f"✅ Schema  : {CATALOG}.{SCHEMA}")
print(f"✅ Volume  : {VOLUME_PATH}")
