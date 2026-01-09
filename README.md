# 📊 Data Engineering Project: Orders & Weather Analytics Pipeline

##🧠 Project Overview

This project implements an end-to-end data engineering pipeline that integrates e-commerce order data with external weather data to enable time-based analytical insights.

The pipeline follows modern data engineering best practices, including layered data modeling, orchestration, cloud storage, and analytics-ready marts.

⸻

##🏗️ Architecture Overview

External API → S3 → Snowflake (RAW) → dbt (STG → PREP → MART)

Orchestration & Infrastructure

Docker + Docker Compose
        ↓
Apache Airflow (Webserver + Scheduler)
        ↓
PostgreSQL (Airflow metadata DB)

####Technologies Used
	•	Python – API ingestion and data processing
	•	AWS S3 – cloud object storage
	•	Snowflake – cloud data warehouse
	•	dbt – data transformation and modeling
	•	Apache Airflow – orchestration and scheduling
	•	Docker & Docker Compose – containerized infrastructure
	•	PostgreSQL – Airflow metadata database
	•	SQL – analytical transformations

⸻

##📥 Data Sources
	1.	E-commerce Orders Dataset
	•	Orders, customers, delivery timestamps
	•	Granularity: order-level events
	2.	Weather API (Open-Meteo)
	•	Hourly weather data (temperature, humidity, precipitation, wind)
	•	Location: Rio de Janeiro
	•	Granularity: hourly

⸻

##🧱 Data Modeling Strategy

The project follows a layered modeling approach:

🔹 RAW Layer
	•	Stores data as ingested
	•	Minimal transformation
	•	Schema mirrors source systems

🔹 STAGING (STG)
	•	Type casting
	•	Column renaming
	•	Timestamp normalization
	•	No business logic

🔹 PREP Layer
	•	Joins between orders, customers, and weather
	•	Alignment on hour-level grain using date_trunc('hour')
	•	Clean, analytics-ready intermediate models

🔹 MART Layer
	•	Business-focused datasets
	•	Aggregations (hourly / daily)
	•	Designed for BI tools and analysts

⸻

##⏱️ Time Handling & Grain Design

A key challenge addressed in the project is time alignment across heterogeneous data sources.
	•	All joins are performed using hourly timestamps
	•	date_trunc('hour', timestamp) is used to ensure consistent grain
	•	Avoided joining on TIME or numeric hour values
	•	Ensured stable and reproducible analytics

⸻

##🔁 Orchestration (Airflow)
	•	Airflow DAG schedules daily weather ingestion
	•	Python tasks:
	•	Fetch weather data from API
	•	Upload CSV files to S3
	•	Downstream transformations handled in Snowflake via dbt

⸻

##📈 Example Analytical Use Cases
	•	Order volume vs weather conditions
	•	Impact of precipitation on delivery timing
	•	Hourly order patterns correlated with temperature
	•	Regional customer behavior analysis

⸻

##✅ Key Engineering Practices Demonstrated
	•	Separation of ingestion, storage, and transformation
	•	Schema-on-read via dbt
	•	Reproducible transformations
	•	Clear data grain definition
	•	Analytics-ready marts
	•	Cloud-native architecture

⸻

