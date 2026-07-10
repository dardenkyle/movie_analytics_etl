# movie_analytics dbt project

Transforms IMDb raw tables loaded by the ingestion scripts into analytics-ready marts.

## Layout

- **staging** (`models/staging/`): cleaned/renamed sources from the `raw` schema
- **marts** (`models/marts/`): business-facing tables for analysis

Typical inventory in this project:

| Layer | Models (examples) |
| --- | --- |
| Staging | title basics, ratings, akas, principals, names |
| Marts | title performance, people credits, and related aggregates |

(Exact model filenames live under `models/` — run `dbt ls` after `dbt deps`.)

## Run locally

```bash
cd dbt/movie_analytics
dbt deps
dbt run --target dev
dbt test --target dev
```

## Targets

`profiles.yml` defines:

- **dev** — local PostgreSQL for interactive work
- **ci** — CI pipeline target

Point `DBT_PROFILES_DIR` / profile credentials at your Postgres instance that already has the raw schema populated by `ingestion/load_raw.py`.
