# Copilot Instructions for Movie Analytics ETL

## Project Overview

This is an **IMDb data pipeline** using Docker, Postgres, dbt, and Airflow to process movie/TV datasets. The architecture follows a **raw → staging → marts** pattern with TSV file ingestion from IMDb datasets.

## Key Architecture Patterns

### Data Flow

- **Landing zone**: `data_lake/landing/archive/` contains IMDb `.tsv` files (title.basics, title.ratings, name.basics, etc.)
- **Raw ingestion**: TSV files loaded directly into Postgres `raw` schema using `COPY` commands
- **Schema design**: Raw tables use `text` columns to preserve original data, with proper typing deferred to dbt transformations
- **Null handling**: IMDb uses `\N` for NULL values, handled in COPY commands with `NULL '\N'`

### Database Conventions

- **Connection**: Postgres container `movies_postgres` on port 5432, database `analytics`
- **Raw schema**: Direct TSV mappings in `sql/raw_schema.sql` - all columns as `text` type initially
- **Primary keys**: Only on core entities (`tconst` for titles, `nconst` for names)
- **Comma-separated fields**: Raw data preserves IMDb's comma-separated values (genres, professions) for dbt to parse

### Development Workflow

#### Essential Commands

```bash
# Start infrastructure
docker compose up -d

# Create raw schema (one-time setup)
docker compose exec -T postgres psql -U postgres -d analytics -f /sql/raw_schema.sql

# Load data (example pattern)
docker compose exec -T postgres psql -U postgres -d analytics -c \
"COPY raw.title_basics FROM '/data/landing/archive/title.basics.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER true);"
```

#### File Organization

- `docker-compose.yml`: Single Postgres service with volume mounts for `/sql` and `/data`
- `sql/raw_schema.sql`: All raw table definitions in one file
- `data_lake/landing/archive/`: IMDb TSV files (preserve original filenames)
- `ingestion/`: Python scripts for programmatic loading (currently empty)
- `dbt/` and `dags/`: Future homes for transformations and orchestration

## Project-Specific Conventions

### TSV Loading Pattern

Always use this COPY syntax for IMDb files:

```sql
COPY raw.[table_name] FROM '/data/[file_path]' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER true);
```

### Schema Evolution

- Raw tables: Preserve original IMDb column names and use `text` for everything
- Never alter raw schema - all type casting and cleaning happens in dbt staging
- Comma-separated fields stay as single text columns in raw layer

### Docker Volume Strategy

- `/sql` mount: For schema DDL files accessible to Postgres container
- `/data` mount: Maps `data_lake/` for direct file access from Postgres container

## Python Development Standards

### Code Quality

- **Style**: Follow PEP8, use `black` for formatting
- **Linting**: Industry best practices for code quality
- **Virtual environment**: Project uses `.venv/` for dependency isolation
- **Focus**: Prioritize data ingestion implementation before transformations

### Planned dbt Transformation Patterns

- **Staging layer**: Type casting (`text` → proper types), `\N` → NULL conversion
- **Marts layer**: Business logic for movies, ratings, people analytics
- **Comma-separated parsing**: Split genres, professions into normalized tables
- **Data quality**: Tests for referential integrity between titles and names

## Current Development State

- **Implemented**: Postgres setup, raw schema, manual data loading
- **Active priority**: Automated data ingestion pipeline (`ingestion/` scripts)
- **Next phase**: dbt models for staging/marts, Airflow DAG for orchestration
- **Missing components**: `dbt/` project structure, `dags/` Airflow workflows

When working on ingestion, focus on robust error handling for file processing and database connections. IMDb datasets contain quirks like adult content flags, year ranges for TV series, and nested comma-separated data.
