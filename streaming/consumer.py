import os
import json
from kafka import KafkaConsumer

def consume_cdc_events():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_CDC_TOPIC", "neon_cdc.public.customer_transactions")

    consumer = KafkaConsumer(
        topic,
        bootstrap_server_id=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
    )

    print(f"Listening to Kafka topic: {topic}...")
    try:
        for message in consumer:
            payload = message.value
            if not payload:
                continue
            
            op = payload.get("op")
            before = payload.get("before")
            after = payload.get("after")
            
            print(f"[{op.upper() if op else 'UNKNOWN'}] Key: {message.key}")
            print(f"  Before: {before}")
            print(f"  After:  {after}\n")
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_cdc_events()