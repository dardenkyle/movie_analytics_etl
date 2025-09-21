# Movie Analytics ETL

![CI](https://github.com/dardenkyle/movie_analytics_etl/workflows/Movie%20Analytics%20ETL%20Pipeline/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-1.9.1-orange.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Last Commit](https://img.shields.io/github/last-commit/dardenkyle/movie_analytics_etl)

An end-to-end data engineering project using **PostgreSQL and dbt** to process IMDb datasets into a dimensional data warehouse.

## Project Status ✅

**Completed Components:**

- ✅ **Raw Data Ingestion**: 176M+ records loaded from 5 IMDb TSV files
- ✅ **dbt Staging Models**: Type casting, null handling, and data quality filters
- ✅ **Data Quality Framework**: 30 comprehensive tests with 80% pass rate
- ✅ **Referential Integrity**: Fixed orphaned records and missing references
- ✅ **dbt Marts Layer**: Complete dimensional model with 4 business tables
- ✅ **Analytics Dashboard**: Interactive HTML dashboard with 5 visualizations

**Current Status:**

- 📊 **877K+ Movies** and **352K+ TV Series** processed and ready for analysis
- � **14.7M+ People** with career metrics and generational analysis
- 📊 **99K+ High-Quality Ratings** with statistical significance testing
- 📊 **Complete Star Schema** with facts, dimensions, and bridge tables

## Overview

- **Raw ingestion**: IMDb `.tsv` files loaded into PostgreSQL (`raw` schema) - **COMPLETED**
- **Transformations**: dbt staging models with data quality improvements - **COMPLETED**
- **Data Mart**: Dimensional warehouse (`staging_marts` schema) - **COMPLETED**
- **Testing**: Comprehensive data quality and integrity tests - **COMPLETED**
- **Analytics**: Business-ready dashboard and sample queries - **COMPLETED**

## Quickstart

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ with virtual environment

### 1. Start Infrastructure

```bash
docker compose up -d
```

### 2. Set Up Raw Schema (One-time)

```bash
docker compose exec -T postgres \
psql -U postgres -d analytics -f /sql/raw_schema.sql
```

### 3. Load IMDb Data

**Option A: Automated Loading (Recommended)**

```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows
# or: source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install psycopg2-binary

# Load all datasets
python ingestion/load_raw.py
```

**Option B: Manual Loading (Single Table Example)**

```bash
docker compose exec -T postgres \
psql -U postgres -d analytics -c \
"COPY raw.title_basics FROM '/data/landing/archive/title.basics.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER true);"
```

### 4. Run Complete Pipeline (Automated)

**Recommended: Use the automated pipeline script**

```bash
# Windows
run_pipeline.bat

# Linux/Mac
chmod +x run_pipeline.sh
./run_pipeline.sh
```

This script will:

- ✅ Check prerequisites and start PostgreSQL
- ✅ Set up Python environment and dependencies
- ✅ Load raw IMDb data (if not already loaded)
- ✅ Run all dbt transformations (staging → marts)
- ✅ Execute comprehensive data quality tests
- ✅ Generate documentation

**Manual Option: Run dbt Transformations Step-by-Step**

```bash
cd dbt/movie_analytics

# Install dbt and dependencies
pip install -r ../../requirements.txt
dbt deps

# Build staging models
dbt run --select staging

# Build marts models
dbt run --select marts

# Run data quality tests
dbt test
```

## 📊 Analytics & Visualization

> **Note**: This project focuses primarily on **data engineering** rather than analytics. While a functional dashboard is provided for demonstration purposes, there are known visualization issues (Plotly binary encoding) that will be addressed in future iterations. The core data engineering pipeline, dimensional modeling, and data quality are complete and production-ready.

### Interactive Dashboard

```bash
# Create comprehensive analytics dashboard
python analytics/create_dashboard.py

# Open movie_analytics_dashboard.html in your browser for:
# - Movie production trends by decade
# - Genre popularity analysis
# - Quality vs popularity scatter plots
# - Runtime evolution over time
# - Entertainment industry generational analysis
```

### Explore Your Data Warehouse

```bash
# View interactive dbt documentation
cd dbt/movie_analytics
dbt docs generate
dbt docs serve

# Run sample business queries
# See analytics/sample_queries.md for 12 ready-to-use analytical queries
```

### Available Data Marts

- **`dim_titles`**: 1.2M+ movies/shows with content categorization
- **`dim_people`**: 14.7M+ industry professionals with career metrics
- **`fact_ratings`**: Quality metrics with statistical significance flags
- **`bridge_cast_crew`**: Cast/crew relationships with role categorization

### Sample Insights Available:

- **Content trends**: Movie production by decade, genre popularity evolution
- **Quality analysis**: Rating distributions, highly-rated vs popular content
- **People analytics**: Career spans, generational shifts, profession distributions
- **Business metrics**: Success categories, statistical significance testing

## Project Structure

```
movie_analytics_etl/
├── docker-compose.yml           # Postgres container with volume mounts
├── sql/raw_schema.sql          # Raw table definitions for IMDb data
├── data_lake/landing/archive/  # IMDb .tsv files (176M+ records)
│   ├── title.basics.tsv       # ~10M titles (movies, TV shows, etc.)
│   ├── title.ratings.tsv      # ~1.4M ratings
│   ├── name.basics.tsv        # ~13M people (actors, directors, etc.)
│   ├── title.principals.tsv   # ~57M cast/crew relationships
│   └── title.akas.tsv         # ~94M alternative titles
├── ingestion/load_raw.py      # Automated data loading script
├── analytics/                 # Business analytics and dashboards
│   ├── create_dashboard.py   # Interactive HTML dashboard generator
│   ├── sample_queries.md     # 12 business intelligence queries
│   └── movie_analytics_dashboard.html  # Generated visualization
├── dbt/movie_analytics/       # dbt project with complete dimensional model
│   ├── models/staging/        # 5 staging models with data quality
│   ├── models/marts/          # 4 business-ready dimensional tables
│   ├── models/sources.yml     # Raw data source definitions
│   └── tests/                 # Custom data quality tests
└── .github/                   # AI agent instructions and project context
```

## Data Quality & Testing

Our dbt project includes comprehensive data quality measures:

- **30 Total Tests**: Covering nulls, uniqueness, relationships, and business rules
- **80% Pass Rate**: 24 tests passing, 6 edge cases identified
- **Referential Integrity**: Foreign key relationships maintained across all tables
- **Data Filters**: Automatic handling of orphaned records and invalid values

### Key Data Quality Improvements

1. **Missing Names**: Filtered 59 records with null `primary_name`
2. **Invalid Types**: Removed 1 record with unsupported `title_type` ('tvPilot')
3. **Orphaned Records**: Handled 9,943 orphaned alternative titles and 9,180 cast/crew records
4. **Null Handling**: Proper conversion of IMDb's `\N` values to SQL nulls

## Next Steps & Roadmap

### Immediate Enhancements

- **Performance Optimization**:
  - Optimize `bridge_cast_crew` table build performance
  - Add database indexes for faster analytical queries
  - Implement incremental dbt models for large tables

### Short Term

- **Advanced Analytics**:
  - Network analysis of actor-director collaborations
  - Time-series forecasting of industry trends
  - Recommendation engine based on ratings similarity

### Long Term

- **Data Expansion**: Additional datasets (Box Office, Awards, Reviews)
- **Real-time Pipeline**: Streaming updates for new releases
- **API Layer**: RESTful endpoints for dashboard integration
- **Cloud Migration**: AWS/GCP deployment with managed services

## Architecture Highlights

- **Dimensional Modeling**: Star schema following Kimball methodology
- **Data Quality**: Comprehensive testing with 80%+ pass rate
- **Scalable Design**: Handles 176M+ records with room for growth
- **Modern Stack**: dbt + PostgreSQL + Python analytics
- **Documentation**: Self-documenting with dbt and inline comments

## Development Notes

- **IMDb Data Quirks**: Uses `\N` for nulls, comma-separated values, adult content flags
- **Memory Requirements**: Large datasets require Docker memory allocation (4GB+ recommended)
- **dbt Best Practices**: Raw → Staging → Marts pattern with comprehensive testing
- **VS Code Setup**: dbt Power User extension recommended for Jinja support

---

Built with ❤️ for learning modern data engineering patterns
