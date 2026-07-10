"""
Shared PostgreSQL connection configuration.

Reads connection settings from POSTGRES_* environment variables with
local-dev defaults matching docker-compose.yml, mirroring the env_var()
pattern used by dbt/movie_analytics/profiles.yml. The CI workflow
exports these variables explicitly.
"""

import os
from typing import Any, Dict

DEFAULT_PORT = 5432


def _read_port() -> int:
    """
    Read POSTGRES_PORT from the environment.

    Unset, empty, or whitespace-only values fall back to the default.

    Returns:
        int: PostgreSQL port number

    Raises:
        ValueError: If POSTGRES_PORT is set to a non-integer value
    """
    raw = os.environ.get("POSTGRES_PORT", "").strip()
    if not raw:
        port = DEFAULT_PORT
    else:
        try:
            port = int(raw)
        except ValueError:
            raise ValueError(f"POSTGRES_PORT must be an integer, got {raw!r}") from None
    return port


def load_db_config() -> Dict[str, Any]:
    """
    Build psycopg2 connection parameters from the environment.

    Returns:
        dict: Keyword arguments for psycopg2.connect()
    """
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": _read_port(),
        "database": os.environ.get("POSTGRES_DB", "analytics"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


DB_CONFIG: Dict[str, Any] = load_db_config()
