import os
from pyspark.sql.functions import col, current_timestamp
from streaming.spark_session import create_spark_session


def run_bronze(spark=None):
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_CDC_TOPIC", "neon_cdc.public.customer_transactions")
    bucket = os.getenv("MINIO_BUCKET", "cdc-lake")

    bronze_path = f"s3a://{bucket}/bronze/customer_transactions"
    checkpoint_path = f"s3a://{bucket}/checkpoints/bronze"

    if spark is None:
        spark = create_spark_session("CDC-Bronze-Writer")

    print(f"Reading stream from Kafka topic: {kafka_topic}")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = (
        kafka_stream.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

    print(f"Writing Bronze Delta stream to: {bronze_path}")

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_path)
    )

    return query


if __name__ == "__main__":
    spark = create_spark_session("CDC-Bronze-Writer")
    spark.sparkContext.setLogLevel("WARN")
    query = run_bronze(spark)
    query.awaitTermination()