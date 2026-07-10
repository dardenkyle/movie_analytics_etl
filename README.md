# Movie Analytics ETL

![CI](https://github.com/dardenkyle/movie_analytics_etl/workflows/Movie%20Analytics%20ETL%20Pipeline/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-1.9.1-orange.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Last Commit](https://img.shields.io/github/last-commit/dardenkyle/movie_analytics_etl)

An end-to-end data engineering pipeline that ingests the public IMDb
datasets (176M+ records across 5 files) into PostgreSQL and transforms
them with dbt into a dimensional data warehouse following Kimball
methodology.

## Architecture

```
IMDb TSV files (data lake landing zone)
        |
        v
PostgreSQL raw schema          (bulk COPY via Python loader)
        |
        v
dbt staging models             (type casting, null handling, quality filters)
        |
        v
dbt marts - star schema        (facts, dimensions, bridge table)
        |
        v
Analytics                      (sample queries, Plotly dashboard)
```

- **Ingestion**: Python loader (`ingestion/load_raw.py`) bulk-loads
  five IMDb TSV files into a `raw` schema using PostgreSQL `COPY`,
  with environment detection so the same script runs locally against
  Docker or inside CI.
- **Staging**: Five dbt staging models handle type casting, conversion
  of IMDb's `\N` null convention, and data quality filtering.
- **Marts**: A star schema with dimension, fact, and bridge tables,
  tested and documented with dbt.
- **Orchestration**: A single pipeline script (`run_pipeline.sh` /
  `run_pipeline.bat`) provisions infrastructure, loads data, runs all
  transformations, and executes the test suite.

## Data Model

| Table | Grain | Scale |
|---|---|---|
| `dim_titles` | One row per title (movie, series, etc.) | 1.2M+ titles with content categorization |
| `dim_people` | One row per industry professional | 14.7M+ people with career metrics |
| `fact_ratings` | One row per rated title | 99K+ ratings with statistical significance flags |
| `bridge_cast_crew` | One row per person-title relationship | Cast/crew links with role categorization |

Source data volumes: ~10M titles, ~1.4M ratings, ~13M people, ~57M
cast/crew relationships, and ~94M alternative titles.

## Tech Stack

- **PostgreSQL 16** (Docker) as the warehouse
- **dbt 1.9** for transformations, testing, and documentation
- **Python 3.11** with psycopg2 for ingestion
- **uv** for dependency management (`pyproject.toml` + `uv.lock`)
- **GitHub Actions** for CI with pre-commit and ruff for code quality

## Quickstart

### Prerequisites

- Docker and Docker Compose
- [uv](https://docs.astral.sh/uv/) (manages Python 3.11+ and all
  project dependencies)
- IMDb datasets (https://datasets.imdbws.com/) extracted to
  `data_lake/landing/archive/`

### 1. Start infrastructure

```bash
docker compose up -d
```

### 2. Create the raw schema (one time)

```bash
docker compose exec -T postgres \
  psql -U postgres -d analytics -f /sql/raw_schema.sql
```

### 3. Load IMDb data

```bash
# Install dependencies (creates .venv from uv.lock)
uv sync

uv run python ingestion/load_raw.py
```

### 4. Run the pipeline

Automated (recommended):

```bash
./run_pipeline.sh               # Linux/Mac
run_pipeline.bat                # Windows
```

The script checks prerequisites, starts PostgreSQL, sets up the Python
environment, loads raw data if needed, runs all dbt transformations,
executes the data quality test suite, and generates documentation.

Manual, step by step:

```bash
cd dbt/movie_analytics
uv run dbt deps
uv run dbt run --select staging
uv run dbt run --select marts
uv run dbt test
```

### 5. Explore the warehouse

```bash
cd dbt/movie_analytics
uv run dbt docs generate
uv run dbt docs serve
```

`analytics/sample_queries.md` contains 12 ready-to-run analytical
queries covering production trends, genre popularity, quality vs.
popularity, and career/generational analysis. A Plotly dashboard
generator is included (`analytics/create_dashboard.py`) for
demonstration purposes; the project's focus is the data engineering
pipeline rather than the visualization layer.

## Data Quality and Testing

The dbt project includes 30 tests covering null constraints,
uniqueness, referential integrity, accepted values, and custom
business rules (rating ranges, plausible release years, runtime and
career-span sanity checks, rating/success consistency).

Notable data quality issues identified and handled in staging:

- Filtered 59 person records with null `primary_name`
- Removed records with unsupported title types
- Resolved ~9,900 orphaned alternative titles and ~9,200 orphaned
  cast/crew records to preserve referential integrity
- Normalized IMDb's `\N` sentinel values to SQL nulls

## CI/CD

The GitHub Actions pipeline ([workflow](.github/workflows/main.yml))
runs on every push and pull request:

1. **Code quality**: pre-commit hooks with ruff linting and
   formatting
2. **Infrastructure validation**: spins up PostgreSQL 16 as a service
   container, applies the raw schema, and verifies database
   connectivity from the loader
3. **dbt validation**: parses and compiles all models and tests
   against a dedicated CI target, then generates documentation

CI validates schema and infrastructure rather than transformation
logic on full data, keeping runs under five minutes with no large
file dependencies. The rationale and trade-offs are documented in
[TESTING_STRATEGY.md](TESTING_STRATEGY.md).

## Project Structure

```
movie_analytics_etl/
├── docker-compose.yml            # PostgreSQL 16 container with volume mounts
├── pyproject.toml                # Project metadata, dependencies, tool config
├── uv.lock                       # Locked dependency versions (managed by uv)
├── sql/raw_schema.sql            # Raw table definitions for IMDb data
├── data_lake/landing/archive/    # IMDb .tsv files (176M+ records, gitignored)
├── ingestion/
│   ├── config.py                 # Shared DB config from POSTGRES_* env vars
│   ├── load_raw.py               # Bulk loader with environment detection
│   └── load_test_data.py         # CI-specific test data loader
├── dbt/movie_analytics/
│   ├── models/staging/           # 5 staging models with quality filters
│   ├── models/marts/             # Star schema: dims, fact, bridge
│   ├── models/sources.yml        # Raw source definitions
│   └── tests/                    # Custom business-rule tests
├── analytics/
│   ├── sample_queries.md         # 12 analytical queries
│   └── create_dashboard.py       # Plotly dashboard generator
├── .github/workflows/main.yml    # CI pipeline
├── .pre-commit-config.yaml       # Formatting and lint hooks
└── TESTING_STRATEGY.md           # CI/CD testing methodology
```

## Roadmap

- Incremental dbt models and index tuning for the largest tables
  (`bridge_cast_crew` in particular)
- Sample-data CI stage to exercise transformation logic end to end
- Additional datasets (box office, awards) and a serving/API layer
- Cloud deployment on managed services (AWS/GCP)

## Development Notes

- IMDb data uses `\N` for nulls and comma-separated multi-value
  fields; staging models normalize both.
- Loading the full datasets requires 4GB+ of memory allocated to
  Docker.
- The dbt Power User extension for VS Code is recommended for Jinja
  support.

## License

MIT - see [LICENSE](LICENSE).
