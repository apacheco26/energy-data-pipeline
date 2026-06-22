from sqlalchemy import create_engine, text
import os

engine = create_engine(os.environ["DATABASE_URL"])


def init_fetch_log():
    """Create fetch_log table if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                source     TEXT NOT NULL,
                state      TEXT NOT NULL DEFAULT 'ALL',
                year       INT  NOT NULL DEFAULT 0,
                status     TEXT NOT NULL,
                fetched_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (source, state, year)
            )
        """))
        conn.commit()


def is_fetched(source, state="ALL", year=0):
    """Return True if source/state/year is already logged as success or no_data."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT status FROM fetch_log
            WHERE source = :source AND state = :state AND year = :year
        """), {"source": source, "state": state, "year": year})
        row = result.fetchone()
        return row is not None and row[0] in ("success", "no_data")


def log_fetch(source, status, state="ALL", year=0):
    """Insert or update a fetch log entry."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO fetch_log (source, state, year, status)
            VALUES (:source, :state, :year, :status)
            ON CONFLICT (source, state, year)
            DO UPDATE SET status = EXCLUDED.status, fetched_at = now()
        """), {"source": source, "state": state, "year": year, "status": status})
        conn.commit()


def check_data_exists(table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar() > 0
    except Exception:
        return False
