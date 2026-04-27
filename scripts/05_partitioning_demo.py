# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Partitioning Demo
# MAGIC
# MAGIC Memperlihatkan **kapan partitioning HANCURKAN** performa, dan
# MAGIC bandingkan dengan **liquid clustering**.
# MAGIC
# MAGIC Aturan resmi:
# MAGIC > Jangan partition kalau tabel < 1 TB.
# MAGIC > Jangan partition kalau tiap partition < 1 GB.
# MAGIC > Untuk tabel baru → pakai Liquid Clustering.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
# Bad partitioning: by customer_id (high cardinality → JUTAAN partisi kecil).
spark.sql(f"DROP TABLE IF EXISTS {FQN('sales_bad_partition')}")
(spark.table(FQN("sales_raw"))
      .write.mode("overwrite")
      .partitionBy("customer_id")        # ⚠️ jangan ditiru di production
      .saveAsTable(FQN("sales_bad_partition")))

# COMMAND ----------
# Good partitioning: by order_date (cardinality ~1000, tiap partisi besar).
spark.sql(f"DROP TABLE IF EXISTS {FQN('sales_good_partition')}")
(spark.table(FQN("sales_raw"))
      .write.mode("overwrite")
      .partitionBy("order_date")
      .saveAsTable(FQN("sales_good_partition")))

# COMMAND ----------
# Bandingkan jumlah file:
for tbl in ["sales_bad_partition", "sales_good_partition", "sales_clustered"]:
    detail = spark.sql(f"DESCRIBE DETAIL {FQN(tbl)}").collect()[0].asDict()
    print(f"{tbl:25s}  numFiles={detail['numFiles']:>10}   "
          f"sizeMB={detail['sizeInBytes']/1024/1024:>10.1f}")

# COMMAND ----------
# MAGIC %md
# MAGIC Kesimpulan: high-cardinality partitioning = ribuan file kecil.
# MAGIC Untuk filter point-lookup, **liquid clustering** jauh lebih baik.
