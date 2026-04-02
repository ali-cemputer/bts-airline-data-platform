# BTS Airline On-Time Performance — Data Engineering Pipeline

Bu belge, uçtan uca data engineering pipeline'ının tüm adımlarını takip etmek için kullanılır.

---

## FAZ 0 — Hazırlık & Ortam Kurulumu

- [x] GitHub repo oluşturuldu, branch stratejisi belirlendi (`main` / `dev`)
- [x] Proje klasör yapısı oluşturuldu: `ingestion/` `processing/` `analytics/` `infra/` `docs/`
- [x] `uv init` ile Python ortamı kuruldu, `pyproject.toml` ve `uv.lock` oluşturuldu
- [x] `.env.example` dosyası yazıldı (GCP project id, bucket name, BQ dataset, servis hesabı path)
- [x] GCP hesabında proje oluşturuldu, servis hesabı ve JSON key indirildi

---

## FAZ 1 — IaC: Cloud Altyapı (Terraform)

- [x] Terraform proje yapısı kuruldu: `main.tf` · `variables.tf` · `outputs.tf` · `terraform.tfvars`
- [x] GCS bucket kaynakları tanımlandı (bronze layer)
- [x] BigQuery dataset kaynağı tanımlandı (gold layer)
- [x] IAM servis hesabı rolleri tanımlandı (Storage Admin, BQ Admin)
- [x] `terraform init → plan → apply` çalıştırıldı, GCP altyapısı doğrulandı

---

## FAZ 2 — Data Ingestion Pipeline

- [x] Airflow için `Dockerfile` yazıldı, `docker-compose.yml`'a airflow + postgres servisleri eklendi
- [x] TranStats PREZIP URL'leri keşfedildi, 2023–2025 aylarına ait zip listesi doğrulandı
- [x] `config.py` yazıldı: kolon listesi, URL şablonu, DB bağlantı bilgileri tek yerde toplandı
- [x] `utils.py` yazıldı: DB bağlantısı ve logging tüm scriptler arasında paylaşılıyor
- [x] `extract.py` yazıldı: ZIP indir → aç → kolonları filtrele → `data/raw/YYYY/YYYY_MM.csv` kaydet
- [x] `load_raw.py` yazıldı: ham CSV → `raw.carrier_report` (tüm kolonlar TEXT, idempotent)
- [x] `transform_load_staging.py` yazıldı: tip dönüşümü (FLOAT, BOOLEAN, DATE) + `delay_category` türetilmiş kolonu → `staging.carrier_report`
- [x] 2023 Ocak verisi (538.837 satır) raw ve staging'e yüklendi, pgcli ile doğrulandı
- [x] Lookup tabloları (`L_MONTHS`, `L_WEEKDAYS`, `L_UNIQUE_CARRIERS`, `L_AIRPORT`, `L_YESNO_RESP`, `L_CANCELLATION`) ziplenip repoya eklendi
- [x] Kolon açıklamaları `docs/` altında TR ve EN olarak dokümante edildi
- [x] `ingestion/notebooks/EDA_1.ipynb` ile staging verisi üzerinde data profiling yapıldı
- [x] CSV → Parquet dönüşümü yapıldı, GCS `bronze/year=/month=/` yapısına yüklendi
- [x] Ham veri local PostgreSQL'e yüklendi (Spark testi ve geliştirme için)
- [x] Airflow DAG yazıldı: tüm ingestion adımları orchestrate edildi, monthly schedule + backfill desteklendi
- [x] 2023-2025 verileri local, postgre, gcs e yüklendi. DAG başarılı şekilde çalıştı.

---

## FAZ 3 — Data Processing (PySpark)

- [ ] Spark için `Dockerfile` yazıldı (local mode), `docker-compose.yml`'a spark servisi eklendi
- [ ] PySpark scripti: GCS bronze okundu, tip dönüşümleri yapıldı (date, int cast)
- [ ] Null yönetimi: iptal uçuşlarda delay kolonları 0 ile dolduruldu
- [ ] Lookup join'leri uygulandı: airline adı, origin/dest şehir-eyalet
- [ ] Türetilmiş kolonlar üretildi: `is_delayed` (ArrDelay > 15) · `delay_category` (No Delay / Minor / Major / Severe)
- [ ] Silver Parquet GCS `silver/year=/month=/` yapısına yazıldı
- [ ] BigQuery staging tablosuna yüklendi; partition (FlightDate), cluster (Airline + Origin) uygulandı
- [ ] Spark task'ı Airflow DAG'ına eklendi, ingestion downstream'ine bağlandı

---

## FAZ 4 — Analytics: dbt & Dashboard

- [ ] dbt projesi kuruldu: `dbt init`, BigQuery adapter konfigürasyonu, `profiles.yml` ayarlandı
- [ ] `models/staging/` altında staging modeli tanımlandı
- [ ] `mart_delay_by_carrier` modeli yazıldı: havayolu bazında gecikme ve iptal analizi
- [ ] `mart_monthly_trends` modeli yazıldı: 2023–2024 aylık gecikme oranı ve iptal trendi
- [ ] `mart_airport_performance` modeli yazıldı: havalimanı bazında taxi süresi ve on-time performansı
- [ ] dbt testleri yazıldı: `not_null` · `unique` · `accepted_values`
- [ ] Streamlit uygulaması yazıldı: Tile 1 (gecikme nedeni dağılımı) · Tile 2 (aylık trend)
- [ ] Streamlit için `Dockerfile` yazıldı, `docker-compose.yml`'a eklendi

---

## FAZ 5 — Entegrasyon & Reproducibility

- [ ] Tüm servisler tek `docker-compose.yml`'da birleştirildi, network ve volume'lar ayarlandı
- [ ] End-to-end test: temiz ortamda `docker compose up` ile sıfırdan çalıştırıldı
- [ ] README yazıldı: problem tanımı, mimari diyagram, kurulum adımları, partition/cluster açıklaması
- [ ] Değerlendirme kriterleri README'ye göre kontrol edildi, eksikler kapatıldı

---

## Değerlendirme Kriteri Kontrol Listesi

- [ ] Problem net tanımlandı
- [ ] Cloud kullanıldı, IaC ile altyapı kuruldu (Terraform)
- [ ] End-to-end batch pipeline mevcut (Airflow DAG ile orchestrate edildi)
- [ ] Data warehouse tabloları partition ve cluster ile optimize edildi
- [ ] dbt veya Spark ile transformasyon yapıldı
- [ ] En az 2 tile içeren dashboard oluşturuldu
- [ ] README eksiksiz, kod sıfırdan çalışabilir durumda