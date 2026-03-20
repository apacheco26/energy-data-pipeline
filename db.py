from sqlalchemy import create_engine, text
import os
import json


# sends the data fetched to Railway Postgres
# more direct with line code to_sql()

engine = create_engine(os.environ["DATABASE_URL"])

# test connection
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("Database connected successful.")
except Exception as e:
    print(f"Database connection failed; {e}")

def check_data_exists(table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            return count > 0
    except:
        return False

def table_summary(table_name):
    with engine.connect() as conn:
        # scaler function returns a single value
        # so insdead of row object (5030,)
        # the request will retunr 5030
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        col_count = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)).scalar()
        table_size = conn.execute(text(f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))
        """)).scalar()
        print(f"Table: {table_name}")
        print(f"Rows: {row_count}")
        print(f"Columns: {col_count}")
        print(f"Size: {table_size}")

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