# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Sample Data
# MAGIC
# MAGIC Membuat 3 tabel utama (`sales_raw`, `customers`, `products`) yang dipakai di tutorial 03 - 08.
# MAGIC
# MAGIC ⚠️ Default = **100 juta** baris `sales_raw`. Sesuaikan `NUM_ORDERS` jika cluster kecil.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# ---------- KNOBS ----------
NUM_ORDERS    = 100_000_000   # turunkan ke 10_000_000 untuk single-node trial
NUM_CUSTOMERS =   1_000_000
NUM_PRODUCTS  =       5_000
NUM_STORES    =         200
SEED          = 42
# ---------------------------

# COMMAND ----------
# MAGIC %md ## 1. Tabel `customers`

customers = (
    spark.range(NUM_CUSTOMERS)
        .withColumnRenamed("id", "customer_id")
        .withColumn("name",        F.concat(F.lit("Customer_"), F.col("customer_id")))
        .withColumn("email",       F.concat(F.col("customer_id"), F.lit("@example.com")))
        .withColumn("segment",     F.element_at(F.array(F.lit("RETAIL"), F.lit("WHOLESALE"), F.lit("VIP")),
                                                ((F.col("customer_id") % 3) + 1).cast("int")))
        .withColumn("signup_date", F.expr("date_sub(current_date(), cast(rand() * 1000 as int))"))
)
(customers.write.mode("overwrite").saveAsTable(FQN("customers")))
print("✅ customers:", spark.table(FQN("customers")).count())

# COMMAND ----------
# MAGIC %md ## 2. Tabel `products`

categories = ["Electronics", "Fashion", "Grocery", "Home", "Sports", "Toys"]
products = (
    spark.range(NUM_PRODUCTS)
        .withColumnRenamed("id", "product_id")
        .withColumn("name",       F.concat(F.lit("Product_"), F.col("product_id")))
        .withColumn("category",   F.element_at(F.array(*[F.lit(c) for c in categories]),
                                               ((F.col("product_id") % len(categories)) + 1).cast("int")))
        .withColumn("brand",      F.concat(F.lit("Brand_"), (F.col("product_id") % 100)))
        .withColumn("list_price", (F.rand(SEED) * 500 + 5).cast("decimal(10,2)"))
)
(products.write.mode("overwrite").saveAsTable(FQN("products")))
print("✅ products:", spark.table(FQN("products")).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Tabel `sales_raw`
# MAGIC Sengaja **TIDAK** di-cluster / di-partition. Tujuannya: demo "before"
# MAGIC sebelum kita melakukan optimisasi.

# Country distribution sengaja dibikin SKEWED — 60% Indonesia, sisanya tersebar.
country_expr = (
    F.when(F.rand(SEED) < 0.60, F.lit("ID"))
     .when(F.rand(SEED+1) < 0.20, F.lit("MY"))
     .when(F.rand(SEED+2) < 0.20, F.lit("SG"))
     .when(F.rand(SEED+3) < 0.20, F.lit("TH"))
     .when(F.rand(SEED+4) < 0.20, F.lit("VN"))
     .when(F.rand(SEED+5) < 0.20, F.lit("PH"))
     .when(F.rand(SEED+6) < 0.20, F.lit("JP"))
     .when(F.rand(SEED+7) < 0.20, F.lit("KR"))
     .when(F.rand(SEED+8) < 0.20, F.lit("AU"))
     .otherwise(F.lit("US"))
)

sales = (
    spark.range(NUM_ORDERS)
        .withColumnRenamed("id", "order_id")
        .withColumn("customer_id", (F.rand(SEED)   * NUM_CUSTOMERS).cast("int"))
        .withColumn("product_id",  (F.rand(SEED+1) * NUM_PRODUCTS ).cast("int"))
        .withColumn("store_id",    (F.rand(SEED+2) * NUM_STORES   ).cast("int"))
        .withColumn("order_ts",    F.expr("timestamp_seconds(unix_timestamp() - cast(rand() * 60*60*24*365*3 as bigint))"))
        .withColumn("order_date",  F.to_date("order_ts"))
        .withColumn("quantity",    (F.rand(SEED+3) * 19 + 1).cast("int"))
        .withColumn("unit_price",  (F.rand(SEED+4) * 500 + 5).cast("decimal(10,2)"))
        .withColumn("total_amount",(F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
        .withColumn("country",     country_expr)
)

# Sengaja produce banyak file kecil → simulasi "small file problem".
(sales.repartition(800)
      .write.mode("overwrite")
      .saveAsTable(FQN("sales_raw")))

print("✅ sales_raw:", spark.table(FQN("sales_raw")).count())

# COMMAND ----------
# MAGIC %md ## 4. Quick sanity-check
display(spark.sql(f"""
    SELECT country, count(*) AS row_count
    FROM {FQN('sales_raw')}
    GROUP BY country ORDER BY row_count DESC
"""))
