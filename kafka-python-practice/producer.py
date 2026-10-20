import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

orders = [
    {"order_id": 201, "customer": "Amit", "amount": 500, "status": "placed"},
    {"order_id": 202, "customer": "Priya", "amount": 1200, "status": "placed"},
    {"order_id": 203, "customer": "Amit", "amount": 750, "status": "shipped"}
]

for order in orders:
    producer.send("order_events", value=order)
    print("Sent:", order)

producer.flush()
producer.close()
