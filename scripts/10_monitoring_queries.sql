-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 - Monitoring & Cost Optimization
-- MAGIC
-- MAGIC Query siap pakai untuk **Unity Catalog System Tables**.
-- MAGIC Skema yang dipakai sudah diverifikasi terhadap dokumentasi resmi:
-- MAGIC - https://learn.microsoft.com/azure/databricks/admin/system-tables/query-history
-- MAGIC - https://learn.microsoft.com/azure/databricks/admin/system-tables/billing
-- MAGIC - https://learn.microsoft.com/azure/databricks/admin/system-tables/lineage
-- MAGIC
-- MAGIC ⚠️ Beberapa system tables harus di-enable dulu di account console.

-- COMMAND ----------
-- ===== A. Top 20 query paling lambat 7 hari terakhir ====================
-- Catatan: query history hanya berisi query SQL Warehouse + serverless notebooks/jobs.
SELECT
  statement_id,
  executed_by,
  compute.warehouse_id           AS warehouse_id,
  compute.type                   AS compute_type,
  total_duration_ms / 1000.0     AS total_sec,
  read_bytes / 1024.0 / 1024     AS read_mb,
  read_files,
  pruned_files,
  produced_rows,
  left(statement_text, 120)      AS sample_sql
FROM   system.query.history
WHERE  start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND  execution_status = 'FINISHED'
ORDER BY total_duration_ms DESC
LIMIT 20;

-- COMMAND ----------
-- ===== B. DBU consumption per workspace per hari ========================
SELECT
  workspace_id,
  date_trunc('day', usage_start_time) AS day,
  sku_name,
  billing_origin_product,
  sum(usage_quantity) AS dbus
FROM   system.billing.usage
WHERE  usage_start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND  usage_unit = 'DBU'
GROUP BY workspace_id, day, sku_name, billing_origin_product
ORDER BY day DESC, dbus DESC;

-- COMMAND ----------
-- ===== C. Tabel paling sering dibaca/ditulis ===========================
-- Skema benar berdasarkan docs: source_table_* / target_table_*.
SELECT
  coalesce(target_table_full_name, source_table_full_name) AS table_full_name,
  count(*)                   AS event_count,
  count(DISTINCT created_by) AS distinct_users
FROM system.access.table_lineage
WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
  AND coalesce(target_table_full_name, source_table_full_name) IS NOT NULL
GROUP BY table_full_name
ORDER BY event_count DESC
LIMIT 20;

-- COMMAND ----------
-- ===== D. Status Predictive Optimization ================================
SELECT *
FROM   system.storage.predictive_optimization_operations_history
WHERE  start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY start_time DESC
LIMIT 50;

-- COMMAND ----------
-- ===== E. Cek small-file problem pada tabel-tabel kita ==================
-- DESCRIBE DETAIL hanya bisa untuk satu tabel per command.
DESCRIBE DETAIL learn_optimize.tutorial.sales_raw;
-- Lihat numFiles dan sizeInBytes — hitung sizeInBytes / numFiles
-- untuk rata-rata ukuran file. Target ideal: 128 MB - 1 GB.

-- COMMAND ----------
DESCRIBE DETAIL learn_optimize.tutorial.sales_clustered;

-- COMMAND ----------
-- ===== F. Query yang banyak SHUFFLE / SPILL (potensi tuning) ===========
SELECT
  statement_id,
  executed_by,
  total_duration_ms / 1000.0           AS total_sec,
  shuffle_read_bytes  / 1024.0 / 1024  AS shuffle_mb,
  spilled_local_bytes / 1024.0 / 1024  AS spill_mb,
  left(statement_text, 120)            AS sample_sql
FROM   system.query.history
WHERE  start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND  execution_status = 'FINISHED'
  AND  shuffle_read_bytes > 1024L*1024*1024   -- > 1 GB shuffle
ORDER BY shuffle_read_bytes DESC
LIMIT 20;

-- COMMAND ----------
-- ===== G. Disk-cache hit ratio (SQL Warehouse) =========================
SELECT
  date_trunc('day', start_time) AS day,
  avg(read_io_cache_percent)    AS avg_cache_hit_pct,
  count(*)                      AS query_count
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND execution_status = 'FINISHED'
  AND read_bytes > 0
GROUP BY day
ORDER BY day DESC;
