"""
Shared PostgreSQL connection configuration.

Reads connection settings from POSTGRES_* environment variables with
local-dev defaults matching docker-compose.yml, mirroring the env_var()
pattern used by dbt/movie_analytics/profiles.yml. The CI workflow
exports these variables explicitly.
"""

import os
from typing import Any, Dict


def load_db_config() -> Dict[str, Any]:
    """
    Build psycopg2 connection parameters from the environment.

    Returns:
        dict: Keyword arguments for psycopg2.connect()
    """
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "analytics"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


DB_CONFIG: Dict[str, Any] = load_db_config()
