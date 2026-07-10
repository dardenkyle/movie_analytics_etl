"""
IMDb Data Loader

Loads IMDb TSV files into PostgreSQL raw schema using COPY commands.
Follows PEP8 standards and includes robust error handling.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Dict

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

# Mapping of TSV files to database tables
FILE_TABLE_MAPPING = {
    "title.basics.tsv": "raw.title_basics",
    "title.ratings.tsv": "raw.title_ratings",
    "title.akas.tsv": "raw.title_akas",
    "title.principals.tsv": "raw.title_principals",
    "name.basics.tsv": "raw.name_basics",
}

DATA_DIR = Path("data_lake/landing/archive")


def table_identifier(table_name: str) -> sql.Identifier:
    """
    Convert a schema-qualified table name into a safely quoted identifier.

    Args:
        table_name: Table name, optionally schema-qualified (e.g. "raw.title_basics")

    Returns:
        sql.Identifier: Composable identifier for use in SQL statements

    Raises:
        ValueError: If the name is not "table" or "schema.table" with
            non-empty parts
    """
    parts = table_name.split(".")
    if len(parts) > 2 or not all(parts):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return sql.Identifier(*parts)


def get_database_connection():
    """
    Establish connection to PostgreSQL database.

    Returns:
        psycopg2.connection: Database connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info("Successfully connected to database")
        return conn
    except psycopg2.Error as e:
        logger.error("Failed to connect to database: %s", e)
        raise


def check_file_exists(file_path: Path) -> bool:
    """
    Check if TSV file exists and is readable.

    Args:
        file_path: Path to the TSV file

    Returns:
        bool: True if file exists and is readable
    """
    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        return False

    if not file_path.is_file():
        logger.error("Path is not a file: %s", file_path)
        return False

    return True


def clear_table(cursor, table_name: str) -> None:
    """
    Clear existing data from table before loading.

    Args:
        cursor: Database cursor
        table_name: Name of table to clear
    """
    try:
        cursor.execute(
            sql.SQL("TRUNCATE TABLE {table}").format(table=table_identifier(table_name))
        )
        logger.info("Cleared existing data from %s", table_name)
    except psycopg2.Error as e:
        logger.error("Failed to clear table %s: %s", table_name, e)
        raise


def load_tsv_file(cursor, file_path: Path, table_name: str) -> int:
    """
    Load TSV file into database table using COPY command.

    Args:
        cursor: Database cursor
        file_path: Path to TSV file
        table_name: Target database table

    Returns:
        int: Number of rows loaded

    Raises:
        psycopg2.Error: If COPY command fails
    """
    # Detect if running in CI or Docker environment
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"

    if is_ci:
        # CI environment: absolute local path, since server-side COPY
        # resolves relative paths against the Postgres data directory
        container_path = str(file_path.resolve())
    else:
        # Docker environment: use mounted volume path
        container_path = f"/data/landing/archive/{file_path.name}"

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
        logger.info("Loaded %d rows into %s", row_count, table_name)
        return row_count
    except psycopg2.Error as e:
        logger.error("Failed to load %s into %s: %s", file_path.name, table_name, e)
        raise


def verify_data_load(cursor, table_name: str) -> Dict[str, int]:
    """
    Verify data was loaded correctly by checking row count.

    Args:
        cursor: Database cursor
        table_name: Table to verify

    Returns:
        dict: Table statistics
    """
    try:
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=table_identifier(table_name)
            )
        )
        row_count = cursor.fetchone()[0]

        # Get a sample record to verify structure
        cursor.execute(
            sql.SQL("SELECT * FROM {table} LIMIT 1").format(
                table=table_identifier(table_name)
            )
        )
        sample_row = cursor.fetchone()

        return {
            "row_count": row_count,
            "has_data": sample_row is not None,
        }
    except psycopg2.Error as e:
        logger.error("Failed to verify %s: %s", table_name, e)
        return {"row_count": 0, "has_data": False}


def load_all_files() -> Dict[str, bool]:
    """
    Load all IMDb TSV files into the database.

    Returns:
        dict: Results of each file load attempt
    """
    results = {}

    try:
        conn = get_database_connection()
        cursor = conn.cursor()

        for filename, table_name in FILE_TABLE_MAPPING.items():
            file_path = DATA_DIR / filename

            logger.info("Processing %s -> %s", filename, table_name)

            # Check if file exists
            if not check_file_exists(file_path):
                results[filename] = False
                continue

            try:
                # Clear existing data
                clear_table(cursor, table_name)

                # Load new data
                load_tsv_file(cursor, file_path, table_name)

                # Verify load
                stats = verify_data_load(cursor, table_name)

                if stats["row_count"] > 0:
                    logger.info(
                        "Successfully loaded %s: %d rows",
                        filename,
                        stats["row_count"],
                    )
                    results[filename] = True
                else:
                    logger.warning("No data found in %s after load", table_name)
                    results[filename] = False

            except Exception as e:
                logger.error("Failed to load %s: %s", filename, e)
                results[filename] = False

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error("Database operation failed: %s", e)
        return {filename: False for filename in FILE_TABLE_MAPPING.keys()}

    return results


def main():
    """Main execution function."""
    logger.info("Starting IMDb data loading process...")

    # Check if data directory exists
    if not DATA_DIR.exists():
        logger.error("Data directory not found: %s", DATA_DIR)
        sys.exit(1)

    # Load all files
    results = load_all_files()

    # Print summary
    logger.info("\n=== LOADING SUMMARY ===")
    successful_loads = sum(results.values())
    total_files = len(results)

    for filename, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info("%-25s %s", filename, status)

    logger.info(
        "Completed: %d/%d files loaded successfully", successful_loads, total_files
    )

    if successful_loads == total_files:
        logger.info("All files loaded successfully")
    else:
        logger.warning("Some files failed to load. Check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
