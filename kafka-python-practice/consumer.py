
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "order_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="python-orders-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Waiting for order messages...")

try:
    for message in consumer:
        order = message.value

        print("Received order:", order)
        print("Order ID:", order["order_id"])
        print("Customer:", order["customer"])
        print("Amount:", order["amount"])
        print("Status:", order["status"])
        print("-" * 30)

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
