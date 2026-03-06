from sqlalchemy import create_engine, text
import os

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

