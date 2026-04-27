# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Auto Loader Demo
# MAGIC
# MAGIC Demo `cloudFiles` source untuk ingest file JSON yang terus berdatangan
# MAGIC ke Unity Catalog Volume.
# MAGIC
# MAGIC Docs: https://learn.microsoft.com/azure/databricks/ingestion/cloud-object-storage/auto-loader/

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import json, random, time, datetime as dt
from pyspark.sql.functions import col

LANDING_PATH    = f"{VOLUME_PATH}/iot_events"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_chk/iot_events"
SCHEMA_PATH     = f"{VOLUME_PATH}/_schema/iot_events"
TARGET_TABLE    = FQN("iot_events_bronze")

dbutils.fs.mkdirs(LANDING_PATH)

# COMMAND ----------
# MAGIC %md ## STEP 1 — Producer kecil: 5 file JSON, 1000 event masing-masing.
def produce(n_files=5, per_file=1000):
    for i in range(n_files):
        events = [{
            "device_id":  random.randint(1, 200),
            "ts":         dt.datetime.utcnow().isoformat(),
            "temp_c":     round(20 + random.random()*15, 2),
            "humidity":   round(40 + random.random()*40, 2),
            "battery":    round(random.random(), 3)
        } for _ in range(per_file)]
        path = f"{LANDING_PATH}/batch_{int(time.time()*1000)}_{i}.json"
        dbutils.fs.put(path, "\n".join(json.dumps(e) for e in events), True)
    print(f"Produced {n_files} files into {LANDING_PATH}")

produce()

# COMMAND ----------
# MAGIC %md ## STEP 2 — Auto Loader stream (trigger Once = micro-batch)
(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", SCHEMA_PATH)
      .option("cloudFiles.inferColumnTypes", "true")
      .load(LANDING_PATH)
      .writeStream
      .option("checkpointLocation", CHECKPOINT_PATH)
      .option("mergeSchema", "true")
      .trigger(availableNow=True)        # process semua file yg ada lalu stop
      .toTable(TARGET_TABLE)).awaitTermination()

display(spark.table(TARGET_TABLE).limit(20))

# COMMAND ----------
# MAGIC %md ## STEP 3 — Tambah file baru, jalankan ulang stream → incremental
produce(n_files=3, per_file=500)

(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", SCHEMA_PATH)
      .load(LANDING_PATH)
      .writeStream
      .option("checkpointLocation", CHECKPOINT_PATH)
      .trigger(availableNow=True)
      .toTable(TARGET_TABLE)).awaitTermination()

print("Total rows:", spark.table(TARGET_TABLE).count())

# COMMAND ----------
# MAGIC %md ## STEP 4 — Aktifkan Liquid Clustering pada bronze table
spark.sql(f"ALTER TABLE {TARGET_TABLE} CLUSTER BY (device_id)")
spark.sql(f"OPTIMIZE {TARGET_TABLE}")
