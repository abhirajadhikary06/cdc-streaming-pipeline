import os
import random
import time
import argparse
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker()
STATUS_CHOICES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]

def get_connection():
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise ValueError("NEON_DATABASE_URL missing from environment variables.")
    return psycopg2.connect(db_url)

def generate_transaction_data():
    qty = random.randint(1, 10)
    price = round(random.uniform(5.0, 500.0), 2)
    return (
        random.randint(100, 999),          # customer_id
        fake.name(),                        # customer_name
        random.randint(1000, 9999),         # product_id
        fake.catch_phrase(),                # product_name
        qty,                                # quantity
        price,                              # unit_price
        round(qty * price, 2),              # total_amount
        random.choice(STATUS_CHOICES)      # status
    )

def insert_single(cursor):
    data = generate_transaction_data()
    cursor.execute("""
        INSERT INTO customer_transactions 
        (customer_id, customer_name, product_id, product_name, quantity, unit_price, total_amount, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING transaction_id;
    """, data)
    tx_id = cursor.fetchone()[0]
    print(f"[INSERT] Created transaction_id: {tx_id}")
    return tx_id

def insert_batch(cursor, count=5):
    data_list = [generate_transaction_data() for _ in range(count)]
    cursor.executemany("""
        INSERT INTO customer_transactions 
        (customer_id, customer_name, product_id, product_name, quantity, unit_price, total_amount, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, data_list)
    print(f"[BATCH INSERT] Created {count} transactions.")

def update_single(cursor):
    cursor.execute("SELECT transaction_id FROM customer_transactions ORDER BY RANDOM() LIMIT 1;")
    res = cursor.fetchone()
    if not res:
        return
    tx_id = res[0]
    new_status = random.choice(STATUS_CHOICES)
    cursor.execute("""
        UPDATE customer_transactions 
        SET status = %s, updated_at = CURRENT_TIMESTAMP 
        WHERE transaction_id = %s;
    """, (new_status, tx_id))
    print(f"[UPDATE] Updated transaction_id {tx_id} -> status: {new_status}")

def delete_single(cursor):
    cursor.execute("SELECT transaction_id FROM customer_transactions ORDER BY RANDOM() LIMIT 1;")
    res = cursor.fetchone()
    if not res:
        return
    tx_id = res[0]
    cursor.execute("DELETE FROM customer_transactions WHERE transaction_id = %s;", (tx_id,))
    print(f"[DELETE] Deleted transaction_id {tx_id}")

def test_rollback(conn, cursor):
    print("[TRANSACTION] Demonstrating ROLLBACK...")
    try:
        tx_id = insert_single(cursor)
        cursor.execute("SELECT 1/0;") # Trigger deliberate error
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ROLLBACK] Successfully rolled back transaction. Reason: {e}")

def run_simulation(seed_count=10, continuous=False, interval=2.0):
    conn = get_connection()
    cursor = conn.cursor()
    
    print(f"--- Seeding initial batch ({seed_count} rows) ---")
    insert_batch(cursor, seed_count)
    conn.commit()

    test_rollback(conn, cursor)

    if not continuous:
        cursor.close()
        conn.close()
        print("Initial seeding complete.")
        return

    print(f"--- Starting continuous CDC simulation (Interval: {interval}s) ---")
    try:
        while True:
            op = random.choices(["INSERT", "UPDATE", "DELETE", "BATCH"], weights=[40, 35, 15, 10])[0]
            if op == "INSERT":
                insert_single(cursor)
            elif op == "UPDATE":
                update_single(cursor)
            elif op == "DELETE":
                delete_single(cursor)
            elif op == "BATCH":
                insert_batch(cursor, count=3)
            
            conn.commit()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Neon Data Simulator for CDC Pipeline")
    parser.add_argument("--seed", type=int, default=10, help="Initial seed record count")
    parser.add_argument("--continuous", action="store_true", help="Run continuous workload stream")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval between continuous ops")
    args = parser.parse_args()

    run_simulation(seed_count=args.seed, continuous=args.continuous, interval=args.interval)