"""
Smart City IoT Traffic Event Producer.
Generates realistic streaming telemetry data simulating urban mobility sensors,
including configurable ratios of data quality anomalies (corrupt JSON, out-of-bounds metrics,
duplicate events, schema drift, and late/future arrivals).
"""

import argparse
import json
import logging
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

# Graceful optional imports
try:
    import pytz
    UTC_TZ = pytz.utc
except ImportError:
    UTC_TZ = timezone.utc

try:
    from faker import Faker
    fake = Faker()
    def get_uuid(): return fake.uuid4()
    def get_lat(): return round(float(fake.latitude()), 6)
    def get_lon(): return round(float(fake.longitude()), 6)
except ImportError:
    def get_uuid(): return str(uuid.uuid4())
    def get_lat(): return round(random.uniform(12.8, 13.2), 6)
    def get_lon(): return round(random.uniform(77.5, 77.8), 6)

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None
    KafkaError = Exception

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("TrafficProducer")

# Urban Road Network Definition
ROADS = [
    {"road_id": "R100", "type": "Highway", "speed_limit": 100, "lanes": 4},
    {"road_id": "R200", "type": "Highway", "speed_limit": 100, "lanes": 4},
    {"road_id": "R300", "type": "Arterial", "speed_limit": 60, "lanes": 3},
    {"road_id": "R400", "type": "City Road", "speed_limit": 50, "lanes": 2},
    {"road_id": "R500", "type": "Local Street", "speed_limit": 40, "lanes": 1},
]

ZONES = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION", "HARBOR"]
WEATHER_CONDITIONS = ["CLEAR", "RAIN", "FOG", "STORM", "SNOW"]
VEHICLE_TYPES = ["SEDAN", "SUV", "TRUCK", "BUS", "MOTORCYCLE", "EV_TAXI"]

# LRU Cache for duplicate vehicle event simulation
RECENT_VEHICLE_CACHE: deque = deque(maxlen=200)


def get_current_utc() -> datetime:
    """Returns current UTC timestamp."""
    return datetime.now(UTC_TZ)


def generate_clean_event() -> Dict[str, Any]:
    """Generates a valid, realistic urban traffic sensor telemetry event."""
    road = random.choice(ROADS)
    zone = random.choice(ZONES)
    weather = random.choices(
        WEATHER_CONDITIONS,
        weights=[0.65, 0.20, 0.08, 0.05, 0.02],
        k=1
    )[0]

    # Congestion logic: CBD & Peak zones during bad weather have higher congestion
    base_congestion = random.randint(1, 3)
    if zone in ["CBD", "TRAINSTATION"] or weather in ["RAIN", "STORM"]:
        base_congestion = min(5, base_congestion + random.randint(1, 2))

    # Speed logic: Congestion slows down vehicles
    speed_factor = max(0.2, (6 - base_congestion) / 5.0)
    nominal_speed = int(road["speed_limit"] * speed_factor * random.uniform(0.7, 1.1))
    actual_speed = max(10, min(140, nominal_speed))

    vehicle_id = get_uuid()
    RECENT_VEHICLE_CACHE.append(vehicle_id)

    event_time = get_current_utc().isoformat()

    return {
        "vehicle_id": vehicle_id,
        "road_id": road["road_id"],
        "city_zone": zone,
        "speed": actual_speed,
        "congestion_level": base_congestion,
        "weather": weather,
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "sensor_id": f"SNS-{random.randint(1000, 9999)}",
        "latitude": get_lat(),
        "longitude": get_lon(),
        "event_time": event_time,
    }


def generate_dirty_event() -> Union[Dict[str, Any], str]:
    """
    Generates an intentional data quality anomaly to test downstream DQ & Quarantine layers.
    Simulates real-world sensor malfunctions, network latency, and serialization faults.
    """
    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "null_vehicle_id",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None

    elif dirty_type == "negative_speed":
        base["speed"] = -1 * random.randint(10, 80)

    elif dirty_type == "extreme_speed":
        base["speed"] = random.randint(220, 500)

    elif dirty_type == "duplicate_vehicle" and RECENT_VEHICLE_CACHE:
        base["vehicle_id"] = random.choice(list(RECENT_VEHICLE_CACHE))

    elif dirty_type == "null_vehicle_id":
        base["vehicle_id"] = None

    elif dirty_type == "late_event":
        # Out-of-order data arriving 30m to 3 hours late
        late_delta = timedelta(minutes=random.randint(30, 180))
        base["event_time"] = (get_current_utc() - late_delta).isoformat()

    elif dirty_type == "future_event":
        # Clock drift / futuristic timestamp
        future_delta = timedelta(minutes=random.randint(15, 120))
        base["event_time"] = (get_current_utc() + future_delta).isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed"] = random.choice(["FAST", "SLOW", "ERROR_CODE_99", "N/A"])

    elif dirty_type == "schema_drift":
        base["road_surface_temp_c"] = round(random.uniform(-5.0, 45.0), 2)
        base["firmware_version"] = "v3.8.1-rc2"

    elif dirty_type == "corrupt_json":
        return "###CORRUPT_SENSOR_PAYLOAD_HEX_DEADBEEF###"

    return base


def create_kafka_producer(bootstrap_servers: str) -> Optional[Any]:
    """Initializes and returns a resilient KafkaProducer instance with retry settings."""
    if KafkaProducer is None:
        raise ImportError("kafka-python package is not installed. Please install with 'pip install kafka-python-ng'")
    logger.info(f"Connecting Kafka Producer to bootstrap servers: {bootstrap_servers}")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else str(v).encode("utf-8"),
        acks=1,
        retries=3,
        max_in_flight_requests_per_connection=5,
        linger_ms=10,
    )


def run_producer(
    bootstrap_servers: str,
    topic: str,
    rate_per_sec: float,
    dirty_ratio: float,
    max_events: Optional[int] = None,
    clean_only: bool = False
):
    """Main event generation streaming loop."""
    producer = create_kafka_producer(bootstrap_servers)
    sleep_interval = 1.0 / max(0.1, rate_per_sec)

    total_sent = 0
    clean_sent = 0
    dirty_sent = 0

    logger.info(
        f"Starting Mobility Producer -> Topic: '{topic}' | Rate: {rate_per_sec} evt/s | "
        f"Dirty Ratio: {0.0 if clean_only else dirty_ratio:.2f}"
    )

    try:
        while True:
            if max_events and total_sent >= max_events:
                logger.info(f"Reached max events limit ({max_events}). Stopping producer.")
                break

            is_dirty = (not clean_only) and (random.random() < dirty_ratio)
            event = generate_dirty_event() if is_dirty else generate_clean_event()

            try:
                if isinstance(event, str):
                    producer.send(topic, value={"raw": event})
                    dirty_sent += 1
                else:
                    producer.send(topic, value=event)
                    if is_dirty:
                        dirty_sent += 1
                    else:
                        clean_sent += 1

                total_sent += 1

                if total_sent % 50 == 0:
                    producer.flush()
                    logger.info(
                        f"Produced {total_sent} events | Clean: {clean_sent} | "
                        f"Dirty/Anomalies: {dirty_sent} ({(dirty_sent/total_sent)*100:.1f}%)"
                    )

            except Exception as err:
                logger.error(f"Kafka Delivery Error: {err}")

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Producer stream interrupted by user. Flushing pending batches...")
    finally:
        if producer:
            producer.flush()
            producer.close()
        logger.info(f"Producer terminated gracefully. Total events published: {total_sent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mobility Lakehouse Real-Time Traffic Producer")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:29092",
        help="Kafka bootstrap servers (default: localhost:29092 for host, kafka:9092 for container)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="traffic-topic",
        help="Target Kafka topic (default: traffic-topic)"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Target event generation rate in events per second (default: 2.0)"
    )
    parser.add_argument(
        "--dirty-ratio",
        type=float,
        default=0.25,
        help="Fraction of generated events that contain data quality anomalies (default: 0.25)"
    )
    parser.add_argument(
        "--clean-only",
        action="store_true",
        help="Emit only pristine clean events (disables anomalies)"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional maximum number of events to produce before exiting"
    )

    args = parser.parse_args()

    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate_per_sec=args.rate,
        dirty_ratio=args.dirty_ratio,
        max_events=args.max_events,
        clean_only=args.clean_only
    )
