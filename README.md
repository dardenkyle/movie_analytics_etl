# Movie Analytics ETL

![Last Commit](https://img.shields.io/github/last-commit/dardenkyle/movie_analytics_etl)

An end-to-end data engineering project using **Postgres, dbt, and Airflow** to process IMDb datasets.

## Overview
- **Raw ingestion**: IMDb `.tsv` files loaded into Postgres (`raw` schema).
- **Transformations**: dbt staging + marts for clean, analysis-ready tables.
- **Orchestration**: Airflow DAG to automate ingestion, transformations, and tests.

## Quickstart

1. **Start Postgres**
   ```bash
   docker compose up -d
   ```

2. **Create raw schema**
   ```bash
   docker compose exec -T postgres \
   psql -U postgres -d analytics -f /sql/raw_schema.sql
   ```

3. **Load data (example for title.basics)**
   ```bash
   docker compose exec -T postgres \
   psql -U postgres -d analytics -c \
   "COPY raw.title_basics FROM '/data/landing/archive/title.basics.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER true);"
   ```

## Project Structure
```
movie_analytics_etl/
├── docker-compose.yml    # Postgres (later: Airflow + dbt)
├── sql/                  # schema DDL files
├── data_lake/            # raw data landing zone
│   └── landing/archive   # IMDb .tsv files
├── dags/                 # airflow DAGs
├── dbt/                  # dbt project
└── ingestion/            # python loaders (optional)
```

## Next Steps
- Add dbt `sources.yml` and staging models.
- Create marts for movies, ratings, and people.
- Add Airflow DAG to orchestrate ingestion → dbt run → dbt test.
