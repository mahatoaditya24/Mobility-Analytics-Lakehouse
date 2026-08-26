"""
Traffic Producer entrypoint (Backwards-compatible alias for traffic_producer.py).
"""
import sys
from producer.traffic_producer import run_producer

if __name__ == "__main__":
    run_producer(
        bootstrap_servers="localhost:29092",
        topic="traffic-topic",
        rate_per_sec=2.0,
        dirty_ratio=0.25
    )