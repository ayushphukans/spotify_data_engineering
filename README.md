# 🎧 Spotify Data Pipeline

This is a full-stack data engineering project that collects Spotify track and artist metadata across multiple European countries using the Spotify API. The data flows through Kafka, is stored in S3, modeled in Snowflake using dbt, and orchestrated via Apache Airflow.

---

## 📦 Prerequisites

Make sure you have the following installed:

- [Docker](https://www.docker.com/) & Docker Compose  
- Spotify Developer Account  
- AWS Account (for S3 access)  
- Snowflake Account
- Preferably create a venv after cloning the repository

---

## 🚀  Highlights

- Kafka producers stream Spotify metadata  
- Kafka consumers push raw JSON to S3  
- Data modeled in Snowflake with dbt (Bronze ➝ Silver ➝ Gold)  
- Airflow orchestrates the entire pipeline  
- Final data ready for dashboards (Power BI, Tableau, etc.)

---

## 🧩 Snowflake & AWS S3 Integration

To enable the pipeline to load data from S3 into Snowflake, you need to create an external stage and storage integration between your Snowflake account and an S3 bucket.

Follow this video for a step-by-step guide:  
📺 [How to Connect AWS S3 with Snowflake](https://www.youtube.com/watch?v=roXlSAvx6Kg&t=742s)

In short, you will:

1. Create an S3 bucket (or use an existing one).
2. Create an IAM role with the required S3 access and trust policy.
3. Set up a **Storage Integration** in Snowflake.
4. Create an **External Stage** in Snowflake referencing your S3 bucket.
5. Grant the necessary privileges to roles and schemas.

Ensure these steps are completed before running the pipeline.

---

## 🛠 How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/spotify-data-pipeline.git
cd spotify-data-pipeline
```
### 2. Add Your Credentials

```bash
cp .env.example .env
```
### 3. Launch the Pipeline Environment

Make sure Docker is running

```bash
docker-compose run airflow airflow db init
docker-compose up --build
```
### 4. Access Airflow & Trigger the Pipeline

Once all services are running, open Airflow in your browser:

[http://localhost:8080](http://localhost:8080)

Log in using your Airflow credentials and trigger the `daily_spotify_pipeline` DAG to start the data pipeline.

---

### ✅ Outputs

After a successful pipeline run:

- Kafka producers send metadata to the broker
- Kafka consumers write raw JSON files to S3
- Snowflake tables are populated via dbt in a Bronze ➝ Silver ➝ Gold format
- Final models are ready for BI tools like Power BI, Tableau, or Looker

You can now use these tables to build rich dashboards and derive music insights across countries.

---

### 🙌 Contributing

If you’d like to contribute or improve this project, feel free to fork the repo and raise a pull request!

---

### 📬 Contact

Have questions or feedback? Feel free to reach out or open an issue.
