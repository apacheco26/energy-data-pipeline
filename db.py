from sqlalchemy import create_engine, text
import os
#import json


# sends the data fetched to Railway Postgres
# more direct with line code to_sql()

engine = create_engine(os.environ["DATABASE_URL"])


def init_fetch_log():
    """Create fetch_log table if it doesn't exist"""
    
    # connect to postgres db
    # create log table if not created
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                source TEXT NOT NULL,
                state  TEXT NOT NULL DEFAULT 'ALL',
                year    INT NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                fetched_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (source, state, year)
            )
        """))
        conn.commit()

# after many breaks in the workflow this controls where 
# to begin after each re-run
def is_fetched(source, state="ALL", year=0):
    """Returns True if already success or no_data"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT status FROM fetch_log
            WHERE source = :source AND state = :state AND year = :year
        """), {"source": source, "state": state, "year": year})
        row = result.fetchone()
        return row is not None and row[0] in ("success", "no_data")
    

def log_fetch(source, status, state="ALL", year=0):
    """Insert or update fetch log entery"""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO fetch_log (source, state, year, status)
            VALUES (:source, :state, :year, :status)
            ON CONFLICT (source, state, year)
            DO UPDATE SET status = EXCLUDED.status, fetched_at = now()
        """), {"source": source, "state": state, "year": year, "status": status})
        conn.commit()

# test connection
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("Database connected successfully.")
except Exception as e:
    print(f"Database connection failed: {e}")

def check_data_exists(table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            return count > 0
    except:
        return False

# reusable function to save any DataFrame to Railway Postgres as JSONB
# no longer save to jsonb, but keeping the function in case we want to save 
# any raw data in the future
def save_to_jsonb(df, table_name, engine):
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                inserted_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()
        records = df.to_dict(orient="records")
        for record in records:
            conn.execute(text(f"""
                INSERT INTO {table_name} (data) VALUES (:data)
            """), {"data": json.dumps(record)})
        conn.commit()
    print(f"{table_name} saved as JSONB")

init_fetch_log()