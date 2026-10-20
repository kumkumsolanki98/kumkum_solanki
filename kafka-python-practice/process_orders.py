
import json
from kafka import KafkaConsumer
import pandas as pd

consumer = KafkaConsumer(
    "order_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
    consumer_timeout_ms=5000,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

orders = []

try:
    for message in consumer:
        order = message.value

        # Basic validation
        required = {"order_id", "customer", "amount", "status"}

        if not required.issubset(order):
            print("Rejected: missing required fields", order)
            continue

        if not isinstance(order["amount"], (int, float)) or order["amount"] < 0:
            print("Rejected: invalid amount", order)
            continue

        orders.append(order)

finally:
    consumer.close()

if orders:
    df = pd.DataFrame(orders)

    # Keep the last record for each order ID in this collected batch
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    output_file = "orders_processed.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Saved {len(df)} unique orders to {output_file}")
    print(df)
else:
    print("No order messages found.")
