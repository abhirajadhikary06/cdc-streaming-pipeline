import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("NEON_DATABASE_URL")

if not db_url:
    raise ValueError("NEON_DATABASE_URL env is missing")

try:
    print("Connecting to Neon PostgreSQL...")
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"Connected successfully to Neon server, version: {db_version[0]}")

    with open("database/neon/01_create_schema.sql", "r") as f:
        schema_sql = f.read()
    
    cursor.execute(schema_sql)
    conn.commit()
    print("Table 'customer_transactions' and dbz_publications created successfully...")

    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'customer_transactions';
    """)
    columns = cursor.fetchall()
    print("\nTable Schema Verification:")
    for col in columns:
        print(f" - {col[0]}: {col[1]}")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error connecting to Neon or creating a schema: {e}")