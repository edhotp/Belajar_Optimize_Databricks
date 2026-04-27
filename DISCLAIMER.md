# ⚠️ Disclaimer

Repository ini dibuat **murni untuk tujuan edukasi dan pembelajaran pribadi** seputar optimasi workload di Azure Databricks.

## Tentang Konten

- Seluruh materi (tutorial & script) adalah **rangkuman & interpretasi** dari dokumentasi publik resmi:
  - [Microsoft Learn — Azure Databricks](https://learn.microsoft.com/azure/databricks/)
  - [Databricks Documentation](https://docs.databricks.com/)
  - [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
  - [Delta Lake Documentation](https://docs.delta.io/latest/)
- Trademark **Azure**, **Databricks**, **Apache Spark**, **Delta Lake**, **Photon**, dan nama produk lain adalah milik pemilik masing-masing. Repo ini **tidak berafiliasi** dengan Microsoft, Databricks, atau Apache Software Foundation.
- Contoh kode bersifat **ilustratif**. Tidak ada jaminan (warranty) atas kebenaran, kelengkapan, performa, atau kesesuaian untuk production. Gunakan dengan risiko sendiri.

## Tentang Data

- **Tidak ada data pribadi (PII), data pelanggan, atau data internal perusahaan** di dalam repo ini.
- Semua dataset di-_generate_ secara sintetis (lihat `scripts/02_generate_sample_data.py`) menggunakan data acak/dummy.
- Tidak ada credential, token, secret, atau connection string yang di-commit. Pastikan kamu **tidak menambahkan** hal tersebut saat fork/PR.

## Biaya Cloud

Menjalankan script di Azure Databricks **akan menimbulkan biaya** (DBU + VM + storage). Pengguna bertanggung jawab penuh atas biaya yang muncul. Selalu **matikan cluster** setelah selesai dan gunakan workspace _trial_ / _free tier_ kalau memungkinkan.

## Lisensi & Kontribusi

Konten dirilis "AS IS" untuk dipelajari & diadaptasi. Jika kamu menemukan kesalahan teknis, silakan buat issue / PR.

---
*Author: pembelajaran pribadi — bukan dokumentasi resmi Microsoft / Databricks.*
