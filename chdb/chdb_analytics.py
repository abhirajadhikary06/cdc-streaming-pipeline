import os
import chdb

def run_gold_analytics():
    # MinIO / S3 Configuration
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket = os.getenv("MINIO_BUCKET", "cdc-lake")

    # MinIO S3 URL pointing to Silver Delta Parquet files
    s3_path = f"http://{minio_endpoint}/{bucket}/silver/customer_transactions/*.parquet"

    # ClickHouse SQL query using s3() table function via chDB engine
    sql_query = f"""
    SELECT 
        customer_id,
        customer_name,
        count(transaction_id) AS total_orders,
        round(sum(total_amount), 2) AS total_spent,
        max(updated_at) AS last_active_at
    FROM s3(
        '{s3_path}',
        '{access_key}',
        '{secret_key}',
        'Parquet'
    )
    GROUP BY customer_id, customer_name
    ORDER BY total_spent DESC
    LIMIT 10
    """

    # Execute query directly inside Python memory space
    result = chdb.query(sql_query, "Pretty")
    print("=== GOLD REAL-TIME AGGREGATIONS (chDB) ===")
    print(result)

if __name__ == "__main__":
    run_gold_analytics()
