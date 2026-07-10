"""
Test data loader for CI environments.
Loads minimal test data instead of full IMDb datasets.
"""

import logging
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ingestion.config import DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Test data mapping
TEST_DATA_MAPPING = {
    "title.basics.tsv": "raw.title_basics",
    "title.ratings.tsv": "raw.title_ratings",
    "title.akas.tsv": "raw.title_akas",
    "title.principals.tsv": "raw.title_principals",
    "name.basics.tsv": "raw.name_basics",
}

DATA_DIR = Path("data_lake/landing/archive")


def table_identifier(table_name: str) -> sql.Identifier:
    """Convert a schema-qualified table name into a safely quoted identifier.

    Raises ValueError unless the name is "table" or "schema.table" with
    non-empty parts.
    """
    parts = table_name.split(".")
    if len(parts) > 2 or not all(parts):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return sql.Identifier(*parts)


def get_database_connection():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info("Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)


def load_test_file(cursor, file_path: Path, table_name: str) -> int:
    """Load test TSV file into database table."""
    # Absolute local path, since server-side COPY resolves relative
    # paths against the Postgres data directory
    container_path = str(file_path.resolve())

    copy_sql = sql.SQL(
        "COPY {table} FROM {path} "
        "WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N', HEADER true)"
    ).format(
        table=table_identifier(table_name),
        path=sql.Literal(container_path),
    )

    try:
        cursor.execute(copy_sql)
        row_count = cursor.rowcount
        logger.info(f"Loaded {row_count} test rows into {table_name}")
        return row_count
    except Exception as e:
        logger.error(f"Failed to load {file_path.name}: {e}")
        return 0


def main():
    """Load test data for CI validation."""
    logger.info("Starting test data loading for CI...")

    # Check if files exist
    missing_files = []
    for filename in TEST_DATA_MAPPING.keys():
        file_path = DATA_DIR / filename
        if not file_path.exists():
            missing_files.append(str(file_path))

    if missing_files:
        logger.error(f"Missing test data files: {missing_files}")
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

    logger.info(f"Test data loading completed. Total rows: {total_rows}")


if __name__ == "__main__":
    main()
