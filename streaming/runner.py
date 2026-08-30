import os
import sys

APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

from streaming.bronze_cdc import run_bronze
from streaming.silver_cdc import run_silver
from streaming.spark_session import create_spark_session


def main():
    spark = create_spark_session("CDC-Pipeline")
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze CDC streaming process...")
    bronze_query = run_bronze(spark)

    print("Starting Silver CDC streaming process...")
    silver_query = run_silver(spark)

    active_queries = [q for q in [bronze_query, silver_query] if q is not None]
    print(f"Successfully started {len(active_queries)} streaming queries in one Spark session.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming pipelines...")
        for query in active_queries:
            query.stop()
        print("Pipeline queries terminated successfully.")


if __name__ == "__main__":
    main()