!(Image/pipline.jpg)

# GHCN Weather Data Architecture (Batch Pipeline)

![Static Badge](https://img.shields.io/badge/Docker-blue?style=for-the-badge&logo=docker&logoColor=blue&labelColor=inactive)
![Static Badge](https://img.shields.io/badge/python-blue?style=for-the-badge&logo=python&logoColor=blue&labelColor=gray)
![Static Badge](https://img.shields.io/badge/APACHE_SPARK-blue?style=for-the-badge&logo=APACHESPARK&logoColor=important%20&labelColor=gray)
![Static Badge](https://img.shields.io/badge/GOOGLE%20CLOUD-red?style=for-the-badge&logo=googlecloud&logoColor=%234285F4&labelColor=gray)
![Static Badge](https://img.shields.io/badge/DBT-core-red?style=for-the-badge&logo=dbt&logoColor=%234285F4&labelColor=gray)

<h2 align="center">End-to-End Modern Data Stack (ELT) Pipeline</h2>

## 📌Project OverView:
Weather conditions heavily influence infrastructures engineering, urban planning and resource managmenet. This project builds an automated data platform that ingest raw data from **the National Centers for Environmental Information (NCEI / NOAA) into a Google Cloud Storage (GCS) Data Lake**
Using *PySpark* raw compressed files(CSV.GZ and text files) undergo lightweight transformation and format conversions to ***parquet*** files before loading to ***BigQuery***.Data is then moldeled using ***dbt*** with built-in data quality test .Finally, business metrics are served through an interactive ***Power BI*** dashboard to analyze temperature , snow depth as well as precipitation patterns.

## 🎯Business Questions:
```
🌨️1: How can determine snow depth and peak of it in Autria?
```
#### Targets:
- civil engineers for constructing bridge and roaf of it near station like Alpline station for safe infrastructure
- municipal urban planners to design resilience infrastructure and system protection

```
🌧️2.How can track and plan precipitation level across different elevation in Austria?
```
#### Targets:
- water resource manager :
- city emergency service : to anticipate urban flood and optimize

```
☀️⛅3. How can find seasonal temperature in Austria in compariosn last year ?
```
### Targets:
- Company Energy
- Agriculture Sectors
- Tourism industy

### 🏗️System Architecture:

```text
+————————————————————————————————————————————————————————————————————————————+
|                          Data Sources                                      |
| 2025.csv.gz | 2024.Csv.gz| ghcnd-countries.txt | ghcnd-stations.txt        |
+————————————————————————————————————————————————————————————————————————————+
                               |
                               ▼
+—————————————————————————————————————————————————————————————————————————————+
|                          Data Lake                                          |
| 2025.parqet| 2024.parquet | ghcnd-countries.parquet| ghcnd-stations.parquet |
+—————————————————————————————————————————————————————————————————————————————+
                               |
                               ▼
+—————————————————————————————————————————————————————————————————————————————+
|                          Data WareHouse                                     |
| GCHN_Weather_dw (load from datalake )                                       |
| GCHN_Weather_dw_staging(Weather, Country, Stations)                         |
| GCHN_Weather_dw_Marts(FCt_Weather, dim_country, dim_station)                |
+—————————————————————————————————————————————————————————————————————————————+
                                |
                                ▼
                          **DashBoard**
```

### 🛠️ Tech Stack

|layer|Technology|Purpose|
|-----|----------|-------|
| **Ingestion** | `Python 3.10+`, `Requests` | Modular extraction scripts with metadata lineage and retry logic |
| **Orchestration** | `Kestra` | Scheduled DAGs, failure alerts, and task dependency management |
| **Data Lake** | `Google Cloud Storage(GCS) ` | storage parquet file  |
| **Processing**|`Pyspark`|lightweight transformation and conversion CSV.GZ to parquet|
| **Data Warehouse** | `BigQuery    |  Serverless analytics engine for structured data storage and SQL querying |
| **Transformation** | `dbt (data build tool)` | Modular SQL modeling, lineage tracking, and schema documentation |
| **Data Quality** | `dbt tests` | Automated uniqueness, non-null, relationship test, accepted value , generic custome assertions|
| **Visualization** | `Power BI` | Interactive BI dashboards  and weather insights in Austria |
| **Infrastructure** | `Docker & Docker Compose` | Fully reproducible local development environment |


### Engineering Decisions in pipline Architucture:
- ⏳pyspark optimization :

    -handling Non-Splittable Compression(.gz): Raw GZIP files created a single partition at stage 0 , forcing 1.2 GB file to process on a single worker core. beacause Adaptive Query Execution (AQE) cannot split GZIP.So, explicit repartition(10) was applied immediately to utilize all cluster cores and accelerate execution.
    - Storage & shuffle Optimization: Before writing final Dataframe to parquet , Applying coalesce (1) instead of repartition to eliminated network shuffle and prevented the Small Files Problem in Google Cloud Storage and minimizing BigQuery costs.

 - 🧪Data Quality & Testing: Data reliability is enforced at every layer using test:

    - Generic assertions:Implemented standard dbt test ('unique','not_null','relationships','accepted_value') across mart models to guarantee data integrity at schema level.

    - Custom Generic Test Macros: developed custom jinja macros to enforce domain- specific business rules.

    - State Comparison & Deferral: leveraged 'manifest.json' state comparison ('dbt run -- state) and deferred execution to process only modified models, significantly reducing pipline execution time and resource consumption

    - Contract Enforcement & Hooks : Enforced strict dbt Model Contracts  in schema changes , utilizing 'pre-hook' and 'post-hook'  scripts for metadata checks.

    - Snapshots(Historical Tracking SCD2): Applied dbt Snapshots to capture slowly changing dimension over time in station model, preserving a full history of updates for station metadata.


### 📂 Project Structure
```
end-to-end-data-pipeline/
|————E-ingest/
|       |——— In_gcs.py
|       |——— Ingest2.py
|
|———L-Pyspark
|       |———— Etl.py
|       |———— Etl2.py
|       |———— Station.py
|       |———— Country.py
|
|————T-DBT
|       |———— Macros
|       |———— Models
|       |        |———— Marts
|       |        |———— Staging
|       |
|       |———— Snapshots
|       |———— dbt_projects
|       |———— requirements.txt
|
|———— Dashboard
|        | Weather.pibx
|        | Weather.pdf
|
|——— .env.exmaple
|———— Docker-compose.yml
|———— kestra.yaml
|———— Requirements.txt
|———— .gitignore
|———— README.md
 ```


### 🚀 Quick Start (Run Locally in 3 Steps)

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* [Git](https://git-scm.com/) installed.

### 1.Clone the repository
```
git clone https://github.com/melikakh2024/pipline-weather.git
cd pipline-weather
```
### 2. Configure environment variables
```
cp .env.example .env
```
### 3. Launch the platform with Docker Compose
```bash
docker compose up -d
```




