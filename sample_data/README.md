# Sample Data

Folder ini hanya berisi **dokumentasi skema** sample data. Data fisik di-generate **langsung di Azure Databricks** menggunakan script di [`../scripts/02_generate_sample_data.py`](../scripts/02_generate_sample_data.py).

Alasannya: dataset yang dipakai bisa mencapai **ratusan juta baris** (untuk benchmarking), sehingga tidak praktis disimpan sebagai file di repo.

## Skema yang akan di-generate

### 1. `sales_raw` (fakta penjualan, ~100 juta row default)
| kolom | tipe | keterangan |
|------|------|----|
| order_id | BIGINT | unik |
| customer_id | INT | high cardinality (~1 juta) |
| product_id | INT | medium cardinality (~5000) |
| store_id | INT | low cardinality (~200) |
| order_ts | TIMESTAMP | rentang 3 tahun |
| order_date | DATE | derived dari `order_ts` |
| quantity | INT | 1-20 |
| unit_price | DECIMAL(10,2) | |
| total_amount | DECIMAL(12,2) | |
| country | STRING | 10 negara, sengaja **skewed** (60% di "ID") |

### 2. `customers` (dimensi, ~1 juta row)
| kolom | tipe |
|------|------|
| customer_id | INT |
| name | STRING |
| email | STRING |
| segment | STRING |
| signup_date | DATE |

### 3. `products` (dimensi, ~5000 row)
| kolom | tipe |
|------|------|
| product_id | INT |
| name | STRING |
| category | STRING |
| brand | STRING |
| list_price | DECIMAL(10,2) |

### 4. `iot_events_raw` (untuk tutorial Auto Loader & streaming)
File JSON kecil-kecil yang ditulis ke volume Unity Catalog setiap menit.
