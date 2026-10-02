"""Stream fake order events into the Kafka 'orders' topic.

Usage:
    python producer.py                    # 1 order/sec, runs until Ctrl+C
    python producer.py --rate 5           # 5 orders/sec
    python producer.py --count 100        # stop after 100 orders
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer

CUSTOMERS = ["alice", "bob", "carla", "dev", "emi", "farid", "gia", "hiro"]
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]


def make_order() -> dict:
    return {
        "order_id": uuid.uuid4().hex[:8].upper(),
        "customer": random.choice(CUSTOMERS),
        "amount": round(random.uniform(5, 500), 2),
        "status": random.choice(STATUSES),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def on_delivery(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"sent -> partition {msg.partition()} offset {msg.offset()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=1.0, help="orders per second")
    parser.add_argument("--count", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order = make_order()
            producer.produce(
                args.topic,
                key=order["order_id"],
                value=json.dumps(order),
                callback=on_delivery,
            )
            producer.poll(0)  # serve delivery callbacks
            sent += 1
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        producer.flush(10)
        print(f"done, {sent} orders produced")


if __name__ == "__main__":
    main()