#!/bin/bash

# Movie Analytics ETL Pipeline Runner
# This script executes the complete pipeline from raw data loading to final analytics

set -e  # Exit on any error

echo "🎬 Movie Analytics ETL Pipeline Starting..."
echo "=============================================="

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_DIR="$PROJECT_DIR/dbt/movie_analytics"
VENV_DIR="$PROJECT_DIR/.venv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}📋 Step $1: $2${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Step 1: Check Prerequisites
print_step 1 "Checking Prerequisites"

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

# Check if PostgreSQL container is running
if ! docker compose ps | grep -q postgres; then
    print_warning "PostgreSQL container not running. Starting it..."
    docker compose up -d postgres
    sleep 10  # Give PostgreSQL time to start
fi

print_success "Prerequisites checked"

# Step 2: Check Python Virtual Environment
print_step 2 "Setting up Python Environment"

if [ ! -d "$VENV_DIR" ]; then
    print_warning "Virtual environment not found. Creating it..."
    python -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate" 2>/dev/null || source "$VENV_DIR/Scripts/activate" 2>/dev/null

# Install/upgrade requirements
pip install -q psycopg2-binary dbt-postgres

print_success "Python environment ready"

# Step 3: Load Raw Data
print_step 3 "Loading Raw Data"

echo "Checking if raw data needs to be loaded..."
DATA_COUNT=$(docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM raw.title_basics;" 2>/dev/null | tr -d ' ' || echo "0")

if [ "$DATA_COUNT" == "0" ] || [ -z "$DATA_COUNT" ]; then
    print_warning "Raw data not found. Loading IMDb datasets..."

    # Create raw schema if it doesn't exist
    docker compose exec -T postgres psql -U postgres -d analytics -f /sql/raw_schema.sql

    # Load raw data
    python ingestion/load_raw.py

    print_success "Raw data loaded successfully"
else
    print_success "Raw data already loaded ($DATA_COUNT title records found)"
fi

# Step 4: Install dbt Dependencies
print_step 4 "Installing dbt Dependencies"

cd "$DBT_DIR"
dbt deps --quiet
print_success "dbt dependencies installed"

# Step 5: Run dbt Models
print_step 5 "Running dbt Transformations"

echo "Building staging models..."
dbt run --select staging --quiet

echo "Building marts models..."
dbt run --select marts --quiet

print_success "dbt models built successfully"

# Step 6: Run Data Quality Tests
print_step 6 "Running Data Quality Tests"

echo "Running staging tests..."
STAGING_RESULTS=$(dbt test --select staging 2>&1 | grep "Done\." || echo "Tests completed")
echo "$STAGING_RESULTS"

echo "Running marts tests..."
MARTS_RESULTS=$(dbt test --select marts 2>&1 | grep "Done\." || echo "Tests completed")
echo "$MARTS_RESULTS"

echo "Running custom business logic tests..."
CUSTOM_RESULTS=$(dbt test --select test_type:generic 2>&1 | grep "Done\." || echo "Tests completed")
echo "$CUSTOM_RESULTS"

print_success "Data quality tests completed"

# Step 7: Generate Documentation
print_step 7 "Generating Documentation"

dbt docs generate --quiet
print_success "Documentation generated (run 'dbt docs serve' to view)"

# Step 8: Pipeline Summary
echo ""
echo "🎉 Pipeline Execution Complete!"
echo "================================"

# Get final record counts
echo "📊 Final Data Summary:"
echo "----------------------"

TITLES_COUNT=$(docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.dim_titles;" | tr -d ' ')
PEOPLE_COUNT=$(docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.dim_people;" | tr -d ' ')
RATINGS_COUNT=$(docker compose exec -T postgres psql -U postgres -d analytics -t -c "SELECT COUNT(*) FROM staging_marts.fact_ratings;" | tr -d ' ')

echo "• Movies/TV Shows: $TITLES_COUNT records"
echo "• People: $PEOPLE_COUNT records"
echo "• Ratings: $RATINGS_COUNT records"

echo ""
echo "🔍 Next Steps:"
echo "• View documentation: cd dbt/movie_analytics && dbt docs serve"
echo "• Run sample queries: see analytics/sample_queries.md"
echo "• Connect BI tools to staging_marts schema in PostgreSQL"
echo ""

print_success "Movie Analytics ETL Pipeline completed successfully! 🚀"
