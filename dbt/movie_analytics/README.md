# movie_analytics (dbt project)

Transforms raw IMDb data in PostgreSQL into a dimensional warehouse.
Models follow a raw -> staging -> marts pattern.

## Layout

- `models/staging/` - five views that cast raw `text` columns to
  proper types, convert IMDb's `\N` sentinel to SQL nulls, and apply
  data quality filters (`stg_title_basics`, `stg_title_ratings`,
  `stg_title_akas`, `stg_title_principals`, `stg_name_basics`)
- `models/marts/` - star schema materialized as tables: `dim_titles`,
  `dim_people`, `fact_ratings`, and the `bridge_cast_crew` bridge
  table
- `models/sources.yml` - source definitions for the `raw` schema
- `tests/` - custom business-rule tests (rating ranges, plausible
  years, runtime and career-span sanity checks)

## Usage

```bash
dbt deps          # install dbt_utils
dbt run           # build staging then marts
dbt test          # schema + custom data quality tests
dbt docs generate && dbt docs serve
```

## Profiles

`profiles.yml` defines two targets:

- `dev` (default) - reads connection settings from `POSTGRES_*`
  environment variables, falling back to the local Docker defaults
- `ci` - fixed localhost settings used by the GitHub Actions service
  container

Set `DBT_PROFILES_DIR` to this directory when running outside of it.
