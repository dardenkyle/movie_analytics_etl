@echo off
REM Movie Analytics ETL Pipeline Runner (Windows)
REM This script executes the complete pipeline from raw data loading to final analytics

echo 🎬 Movie Analytics ETL Pipeline Starting...
echo ==============================================

REM Configuration
set PROJECT_DIR=%~dp0
set DBT_DIR=%PROJECT_DIR%dbt\movie_analytics
set VENV_DIR=%PROJECT_DIR%.venv

echo 📋 Step 1: Checking Prerequisites

REM Check Docker
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if PostgreSQL container is running
docker compose ps | findstr postgres >nul
if %errorlevel% neq 0 (
    echo ⚠️  PostgreSQL container not running. Starting it...
    docker compose up -d postgres
    timeout /t 10 >nul
)

echo ✅ Prerequisites checked

echo 📋 Step 2: Setting up Python Environment

REM Activate virtual environment
if exist "%VENV_DIR%\Scripts\activate.bat" (
    call "%VENV_DIR%\Scripts\activate.bat"
) else (
    echo ⚠️  Virtual environment not found. Please create it first:
    echo python -m venv .venv
    pause
    exit /b 1
)

REM Install/upgrade requirements
pip install -q psycopg2-binary dbt-postgres

echo ✅ Python environment ready

echo 📋 Step 3: Loading Raw Data

echo Checking if raw data needs to be loaded...
for /f %%i in ('docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM raw.title_basics;" 2^>nul ^| findstr /r "[0-9]"') do set DATA_COUNT=%%i

if "%DATA_COUNT%"=="" set DATA_COUNT=0

if %DATA_COUNT%==0 (
    echo ⚠️  Raw data not found. Loading IMDb datasets...

    REM Create raw schema if it doesn't exist
    docker compose exec -T postgres psql -U postgres -d analytics -f /sql/raw_schema.sql

    REM Load raw data
    python ingestion\load_raw.py

    echo ✅ Raw data loaded successfully
) else (
    echo ✅ Raw data already loaded (%DATA_COUNT% title records found^)
)

echo 📋 Step 4: Installing dbt Dependencies

cd /d "%DBT_DIR%"
dbt deps --quiet
echo ✅ dbt dependencies installed

echo 📋 Step 5: Running dbt Transformations

echo Building staging models...
dbt run --select staging --quiet

echo Building marts models...
dbt run --select marts --quiet

echo ✅ dbt models built successfully

echo 📋 Step 6: Running Data Quality Tests

echo Running staging tests...
dbt test --select staging

echo Running marts tests...
dbt test --select marts

echo Running custom business logic tests...
dbt test --select test_type:generic

echo ✅ Data quality tests completed

echo 📋 Step 7: Generating Documentation

dbt docs generate --quiet
echo ✅ Documentation generated (run 'dbt docs serve' to view^)

echo.
echo 🎉 Pipeline Execution Complete!
echo ================================

echo 📊 Final Data Summary:
echo ----------------------

for /f %%i in ('docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.dim_titles;"') do set TITLES_COUNT=%%i
for /f %%i in ('docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.dim_people;"') do set PEOPLE_COUNT=%%i
for /f %%i in ('docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.fact_ratings;"') do set RATINGS_COUNT=%%i

echo • Movies/TV Shows: %TITLES_COUNT% records
echo • People: %PEOPLE_COUNT% records
echo • Ratings: %RATINGS_COUNT% records

echo.
echo 🔍 Next Steps:
echo • View documentation: cd dbt\movie_analytics ^&^& dbt docs serve
echo • Run sample queries: see analytics\sample_queries.md
echo • Connect BI tools to staging_marts schema in PostgreSQL
echo.

echo ✅ Movie Analytics ETL Pipeline completed successfully! 🚀

pause
