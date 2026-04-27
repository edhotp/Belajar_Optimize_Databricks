# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Joins, Broadcast & AQE
# MAGIC
# MAGIC AQE aktif default. Demo:
# MAGIC 1. Sort-merge → Broadcast switch otomatis.
# MAGIC 2. Skew handling (country = "ID" sengaja 60% data).
# MAGIC 3. Coalesce shuffle partitions.
# MAGIC
# MAGIC Docs: https://learn.microsoft.com/azure/databricks/optimizations/aqe

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
# Pastikan AQE on (default true).
print("AQE enabled :", spark.conf.get("spark.databricks.optimizer.adaptive.enabled"))
print("Skew join   :", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("Coalesce    :", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))

# COMMAND ----------
# MAGIC %md ## A. Auto Broadcast Hash Join
# Kita join 100 jt baris sales × 5 ribu baris products.
# Tanpa hint, AQE akan auto-broadcast tabel kecil.

df = spark.sql(f"""
    SELECT s.country, p.category, sum(s.total_amount) rev
    FROM   {FQN('sales_clustered')} s
    JOIN   {FQN('products')}        p USING (product_id)
    GROUP BY s.country, p.category
""")
df.explain(mode="formatted")   # cari node "BroadcastHashJoin"
df.show()

# COMMAND ----------
# MAGIC %md ## B. Force broadcast manual (alternatif kalau AQE tidak auto)
from pyspark.sql.functions import broadcast
sales    = spark.table(FQN("sales_clustered"))
products = spark.table(FQN("products"))

(sales.join(broadcast(products), "product_id")
      .groupBy("category").sum("total_amount")
      .show())

# COMMAND ----------
# MAGIC %md ## C. Demo Skew Handling
# Karena 60% data ber-country='ID', join customer→sales kemungkinan skew.
spark.sql(f"""
    SELECT c.segment, sum(s.total_amount) rev
    FROM   {FQN('sales_clustered')}  s
    JOIN   {FQN('customers')}        c USING (customer_id)
    WHERE  s.country = 'ID'
    GROUP BY c.segment
""").show()
# Lihat di Spark UI: SortMergeJoin akan ada flag isSkew=true → AQE memecah partisi besar.

# COMMAND ----------
# MAGIC %md ## D. Auto-optimized shuffle
spark.conf.set("spark.sql.shuffle.partitions", "auto")
print("shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------
# MAGIC %md ## E. ANALYZE TABLE — bantu CBO
spark.sql(f"ANALYZE TABLE {FQN('sales_clustered')} COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql(f"ANALYZE TABLE {FQN('customers')}       COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql(f"ANALYZE TABLE {FQN('products')}        COMPUTE STATISTICS FOR ALL COLUMNS")
