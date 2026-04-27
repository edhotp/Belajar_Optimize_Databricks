-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - File Sizing & OPTIMIZE
-- MAGIC
-- MAGIC Demo:
-- MAGIC 1. Inspeksi jumlah & ukuran file (small file problem).
-- MAGIC 2. Jalankan `OPTIMIZE` untuk men-compact.
-- MAGIC 3. Bandingkan latency query SEBELUM vs SESUDAH.
-- MAGIC 4. Bersihkan file lama dengan `VACUUM`.
-- MAGIC
-- MAGIC Dokumentasi: https://learn.microsoft.com/azure/databricks/delta/optimize

-- COMMAND ----------
-- ===== STEP 1: lihat jumlah file dan ukuran ==========================
DESCRIBE DETAIL learn_optimize.tutorial.sales_raw;
-- Perhatikan: numFiles biasanya ~800 file kecil, sizeInBytes ~ beberapa GB.

-- COMMAND ----------
-- ===== STEP 2: query baseline (BEFORE OPTIMIZE) ======================
-- Catat waktu di Spark UI / Query Profile.
SELECT country, sum(total_amount) AS revenue
FROM   learn_optimize.tutorial.sales_raw
WHERE  order_date >= date '2024-01-01'
GROUP BY country
ORDER BY revenue DESC;

-- COMMAND ----------
-- ===== STEP 3: OPTIMIZE ==============================================
OPTIMIZE learn_optimize.tutorial.sales_raw;
-- Hasilnya: numFiles turun drastis (target ~256 MB - 1 GB per file),
-- karena Databricks auto-tune ukuran target file.

DESCRIBE DETAIL learn_optimize.tutorial.sales_raw;

-- COMMAND ----------
-- ===== STEP 4: re-run query (AFTER OPTIMIZE) =========================
SELECT country, sum(total_amount) AS revenue
FROM   learn_optimize.tutorial.sales_raw
WHERE  order_date >= date '2024-01-01'
GROUP BY country
ORDER BY revenue DESC;
-- Bandingkan waktu eksekusi & jumlah file yang di-scan di Query Profile.

-- COMMAND ----------
-- ===== STEP 5: Auto Optimize (untuk write berikutnya) ================
ALTER TABLE learn_optimize.tutorial.sales_raw
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------
-- ===== STEP 6: VACUUM file lama (>7 hari default) =====================
-- Default retention 7 hari = aman untuk time-travel. JANGAN turunkan ke 0
-- di production karena dapat menghapus file yang masih dibaca query lain.
VACUUM learn_optimize.tutorial.sales_raw;
