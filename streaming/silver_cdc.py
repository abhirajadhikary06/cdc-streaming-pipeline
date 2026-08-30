import os
import time
from delta.tables import DeltaTable
from pyspark.sql.functions import col, from_json, expr, row_number, unbase64, hex, conv
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)
from streaming.spark_session import create_spark_session

# Define payload schema matching Debezium serialization format
transaction_schema = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", LongType(), True)
])

# Define Debezium CDC envelope schema
cdc_envelope_schema = StructType([
    StructField("before", transaction_schema, True),
    StructField("after", transaction_schema, True),
    StructField("op", StringType(), True)
])


def decode_base64_decimal(col_name):
    """Converts Debezium Big-Endian Base64 Decimal String to PySpark Decimal."""
    return (
        conv(hex(unbase64(col_name)), 16, 10) / 100.0
    ).cast("decimal(12, 2)")


def upsert_to_silver(micro_batch_df, batch_id, silver_path):
    if micro_batch_df.rdd.isEmpty():
        return

    spark = micro_batch_df.sparkSession

    # Parse raw CDC JSON envelope
    parsed_df = (
        micro_batch_df
        .withColumn("cdc", from_json(col("raw_payload"), cdc_envelope_schema))
        .select(
            col("cdc.op").alias("op"),
            col("cdc.before").alias("before"),
            col("cdc.after").alias("after"),
            col("kafka_offset"),
            col("kafka_timestamp")
        )
    )

    # Extract fields & decode base64 binary fields
    processed_df = parsed_df.select(
        col("op"),
        col("kafka_offset"),
        expr("COALESCE(after.transaction_id, before.transaction_id)").alias("transaction_id"),
        expr("CASE WHEN op = 'd' THEN before.customer_id ELSE after.customer_id END").alias("customer_id"),
        expr("CASE WHEN op = 'd' THEN before.customer_name ELSE after.customer_name END").alias("customer_name"),
        expr("CASE WHEN op = 'd' THEN before.product_id ELSE after.product_id END").alias("product_id"),
        expr("CASE WHEN op = 'd' THEN before.product_name ELSE after.product_name END").alias("product_name"),
        expr("CASE WHEN op = 'd' THEN before.quantity ELSE after.quantity END").alias("quantity"),
        decode_base64_decimal(expr("CASE WHEN op = 'd' THEN before.unit_price ELSE after.unit_price END")).alias("unit_price"),
        decode_base64_decimal(expr("CASE WHEN op = 'd' THEN before.total_amount ELSE after.total_amount END")).alias("total_amount"),
        expr("CASE WHEN op = 'd' THEN before.status ELSE after.status END").alias("status"),
        (expr("CASE WHEN op = 'd' THEN before.updated_at ELSE after.updated_at END") / 1000000).cast("timestamp").alias("updated_at")
    )

    # Deduplicate within micro-batch (keep highest offset per primary key)
    window_spec = Window.partitionBy("transaction_id").orderBy(col("kafka_offset").desc())
    latest_events_df = (
        processed_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Initialize Silver Delta Table if not present
    if not DeltaTable.isDeltaTable(spark, silver_path):
        (
            latest_events_df.filter("op != 'd'")
            .drop("op", "kafka_offset")
            .write.format("delta")
            .mode("overwrite")
            .save(silver_path)
        )
        return

    # Execute Delta MERGE (Upsert / Delete)
    silver_table = DeltaTable.forPath(spark, silver_path)
    (
        silver_table.alias("target")
        .merge(
            latest_events_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedDelete(condition="source.op = 'd'")
        .whenMatchedUpdate(
            condition="source.op != 'd'",
            set={
                "customer_id": "source.customer_id",
                "customer_name": "source.customer_name",
                "product_id": "source.product_id",
                "product_name": "source.product_name",
                "quantity": "source.quantity",
                "unit_price": "source.unit_price",
                "total_amount": "source.total_amount",
                "status": "source.status",
                "updated_at": "source.updated_at"
            }
        )
        .whenNotMatchedInsert(
            condition="source.op != 'd'",
            values={
                "transaction_id": "source.transaction_id",
                "customer_id": "source.customer_id",
                "customer_name": "source.customer_name",
                "product_id": "source.product_id",
                "product_name": "source.product_name",
                "quantity": "source.quantity",
                "unit_price": "source.unit_price",
                "total_amount": "source.total_amount",
                "status": "source.status",
                "updated_at": "source.updated_at"
            }
        )
        .execute()
    )


def run_silver(spark=None):
    bucket = os.getenv("MINIO_BUCKET", "cdc-lake")
    bronze_path = f"s3a://{bucket}/bronze/customer_transactions"
    silver_path = f"s3a://{bucket}/silver/customer_transactions"
    checkpoint_path = f"s3a://{bucket}/checkpoints/silver"

    if spark is None:
        spark = create_spark_session("CDC-Silver-Writer")

    while not DeltaTable.isDeltaTable(spark, bronze_path):
        print("Waiting for Bronze Delta table to be initialized...")
        time.sleep(2)

    print(f"Reading stream from Bronze Delta Lake: {bronze_path}")

    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(bronze_path)
    )

    query = (
        bronze_stream.writeStream
        .format("delta")
        .foreachBatch(lambda df, batch_id: upsert_to_silver(df, batch_id, silver_path))
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    return query


if __name__ == "__main__":
    spark = create_spark_session("CDC-Silver-Writer")
    spark.sparkContext.setLogLevel("WARN")
    query = run_silver(spark)
    query.awaitTermination()