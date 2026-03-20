from db import engine, table_summary
from sqlalchemy import text

# show connection info
with engine.connect() as conn:
    db_info = conn.execute(text("SELECT current_database(), inet_server_addr(), inet_server_port()")).fetchone()

print(f"Connected to: {db_info[0]}")
print(f"Host: {db_info[1]}")
print(f"Port:{db_info[2]}")
print("---")

# list and summarize all tables
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """))
    db_tables = [row[0] for row in result]

for table in db_tables:
    table_summary(table)