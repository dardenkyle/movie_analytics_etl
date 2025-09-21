"""
Test data loader for CI environments.
Loads minimal test data instead of full IMDb datasets.
"""

import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG: Dict[str, Any] = {
    "host": "localhost",
    "port": 5432,
    "database": "analytics",
    "user": "postgres",
    "password": "postgres",
}

# Test data mapping
TEST_DATA_MAPPING = {
    "title.basics.tsv": "raw.title_basics",
    "title.ratings.tsv": "raw.title_ratings",
    "title.akas.tsv": "raw.title_akas",
    "title.principals.tsv": "raw.title_principals",
    "name.basics.tsv": "raw.name_basics",
}

DATA_DIR = Path("data_lake/landing/archive")


def get_database_connection():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info("✅ Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        sys.exit(1)


def load_test_file(cursor, file_path: Path, table_name: str) -> int:
    """Load test TSV file into database table."""
    # Use local file paths for CI
    container_path = str(file_path)

    copy_sql = f"""
        COPY {table_name}
        FROM '{container_path}'
        WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N', HEADER true)
    """

    try:
        cursor.execute(copy_sql)
        row_count = cursor.rowcount
        logger.info(f"✅ Loaded {row_count} test rows into {table_name}")
        return row_count
    except Exception as e:
        logger.error(f"❌ Failed to load {file_path.name}: {e}")
        return 0


def main():
    """Load test data for CI validation."""
    logger.info("🔄 Starting test data loading for CI...")

    # Check if files exist
    missing_files = []
    for filename in TEST_DATA_MAPPING.keys():
        file_path = DATA_DIR / filename
        if not file_path.exists():
            missing_files.append(str(file_path))

    if missing_files:
        logger.error(f"❌ Missing test data files: {missing_files}")
        sys.exit(1)

    conn = get_database_connection()
    cursor = conn.cursor()

    total_rows = 0
    for filename, table_name in TEST_DATA_MAPPING.items():
        file_path = DATA_DIR / filename
        rows_loaded = load_test_file(cursor, file_path, table_name)
        total_rows += rows_loaded

    cursor.close()
    conn.close()

    logger.info(f"✅ Test data loading completed! Total rows: {total_rows}")


if __name__ == "__main__":
    main()
