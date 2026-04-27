-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Liquid Clustering (pengganti Z-ORDER)
-- MAGIC
-- MAGIC **Liquid Clustering** adalah rekomendasi resmi Databricks untuk semua
-- MAGIC tabel baru. Lebih fleksibel dari `ZORDER` & `PARTITIONED BY`.
-- MAGIC
-- MAGIC Docs: https://learn.microsoft.com/azure/databricks/delta/clustering

-- COMMAND ----------
-- ===== A. Buat tabel baru dengan Liquid Clustering ====================
CREATE OR REPLACE TABLE learn_optimize.tutorial.sales_clustered
CLUSTER BY (order_date, country) AS
SELECT * FROM learn_optimize.tutorial.sales_raw;

-- Trigger clustering (incremental).
OPTIMIZE learn_optimize.tutorial.sales_clustered;

DESCRIBE DETAIL learn_optimize.tutorial.sales_clustered;
-- Cek properti clusteringColumns.

-- COMMAND ----------
-- ===== B. Query yang akan ter-benefit dari data skipping ==============
SELECT product_id, sum(total_amount) AS rev
FROM   learn_optimize.tutorial.sales_clustered
WHERE  order_date BETWEEN date '2024-06-01' AND date '2024-06-30'
  AND  country = 'ID'
GROUP BY product_id
ORDER BY rev DESC LIMIT 20;

-- Bandingkan "files pruned" dan "files read" di Query Profile.

-- COMMAND ----------
-- ===== C. Mengubah cluster keys (no rewrite) ==========================
ALTER TABLE learn_optimize.tutorial.sales_clustered
CLUSTER BY (customer_id, order_date);

-- Force recluster (HANYA bila data lama juga ingin disusun ulang).
-- OPTIMIZE learn_optimize.tutorial.sales_clustered FULL;

-- COMMAND ----------
-- ===== D. Automatic Liquid Clustering (DBR 15.4 LTS+) =================
-- Databricks akan memilih cluster keys SECARA OTOMATIS dari pola query.
ALTER TABLE learn_optimize.tutorial.sales_clustered CLUSTER BY AUTO;

-- COMMAND ----------
-- ===== E. Comparison: Z-ORDER (legacy) ================================
-- Hanya untuk pemahaman historis. Jangan dipakai untuk tabel baru.
-- OPTIMIZE learn_optimize.tutorial.sales_raw ZORDER BY (order_date, country);
