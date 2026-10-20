
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer

# 1. Create an output folder
output_dir = Path("orders_parquet")
output_dir.mkdir(exist_ok=True)

# 2. Connect to Kafka
consumer = KafkaConsumer(
    "order_events",
    bootstrap_servers="localhost:9092",
    group_id="continuous-orders-v1",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

# 3. Buffer incoming orders
orders = []
last_flush = time.monotonic()
batch_number = 1

print("Listening for order events. Press Ctrl+C to stop.")

def save_batch(records, batch_id):
    """Validate, deduplicate and save one batch."""

    if not records:
        return

    df = pd.DataFrame(records)

    # Remove duplicate order IDs within this batch
    df = df.drop_duplicates(
        subset=["order_id"],
        keep="last"
    )

    # Save each batch as a separate Parquet file
    output_file = output_dir / f"batch_{batch_id:05d}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Saved {len(df)} orders to {output_file}")


try:
    while True:
        # 4. Wait up to 1 second for new messages
        messages = consumer.poll(
            timeout_ms=1000,
            max_records=100
        )

        for partition, records in messages.items():
            for message in records:
                order = message.value

                # 5. Validate the order
                required = {
                    "order_id", "customer", "amount", "status"
                }

                if not required.issubset(order):
                    print("Rejected: missing fields", order)
                    continue

                amount = order["amount"]

                if (
                    isinstance(amount, bool)
                    or not isinstance(amount, (int, float))
                    or amount < 0
                ):
                    print("Rejected: invalid amount", order)
                    continue

                orders.append(order)

        # 6. Save when 10 orders arrive or 5 seconds pass
        elapsed = time.monotonic() - last_flush

        if len(orders) >= 10 or elapsed >= 5:
            save_batch(orders, batch_number)

            orders.clear()
            batch_number += 1
            last_flush = time.monotonic()

            # Commit offsets only after the batch is saved
            consumer.commit()

except KeyboardInterrupt:
    print("\nStopping consumer...")

    # Save remaining valid orders before exiting
    save_batch(orders, batch_number)

    if orders:
        consumer.commit()

finally:
    consumer.close()
    print("Consumer closed.")
