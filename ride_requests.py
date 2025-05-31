from kafka import KafkaProducer
import json
import random
import time
import datetime

# NCCU coordinates as a reference
NCCU_LAT, NCCU_LON = 24.9886, 121.5786

# Simulated passenger IDs
passenger_pool = [2001 + i for i in range(12)]

ZONES = {
    "北投區":   ((25.10, 25.13), (121.45, 121.52)),
    "士林區":   ((25.07, 25.10), (121.50, 121.60)),
    "中山區":   ((25.05, 25.07), (121.52, 121.58)),
    "大同區":   ((25.03, 25.05), (121.49, 121.55)),
    "中正區":   ((25.01, 25.03), (121.50, 121.54)),
    "松山區":   ((25.01, 25.03), (121.54, 121.58)),
    "大安區":   ((24.99, 25.01), (121.50, 121.54)),
    "信義區":   ((24.99, 25.01), (121.54, 121.58)),
    "文山區":   ((24.96, 24.99), (121.50, 121.56)),
    "萬華區":   ((25.00, 25.03), (121.42, 121.48)),
    "內湖區":   ((25.04, 25.09), (121.60, 121.64)),
    "南港區":   ((25.07, 25.12), (121.61, 121.64))
}

def generate_ride_request(passenger_id):

    # Step 1: Choose a zone
    zone = random.choice(list(ZONES.keys()))
    (lat_range, lon_range) = ZONES[zone]

    # Step 2: Generate origin inside selected zone
    origin_lat = round(random.uniform(*lat_range), 7)
    origin_lon = round(random.uniform(*lon_range), 7)

    # Step 3: Generate dropoff location randomly (anywhere in Taipei)
    dest_lat = round(random.uniform(24.96, 25.13), 7)
    dest_lon = round(random.uniform(121.42, 121.64), 7)

    # Step 4: Simulate request metadata
    request_time = datetime.datetime.now().isoformat()
    service_type = random.choice(["快車", "共乘", "尊榮"])

    # Step 5: Return event
    return {
        "event_type": "ride_request",
        "passenger_id": passenger_id,
        "timestamp": request_time,
        "data": {
            "pickup": {"lat": origin_lat, "lon": origin_lon},
            "dropoff": {"lat": dest_lat, "lon": dest_lon},
            "service": service_type,
            "zone": zone
        }
    }

def generate_ride_request(passenger_id):
    # Pick zone uniformly
    zone = random.choice(list(ZONES.keys()))
    (lat_range, lon_range) = ZONES[zone]

    origin_lat = round(random.uniform(*lat_range), 7)
    origin_lon = round(random.uniform(*lon_range), 7)

    # Generate random destination (can fall anywhere)
    dest_lat = round(random.uniform(24.96, 25.13), 7)
    dest_lon = round(random.uniform(121.42, 121.64), 7)

    request_time = datetime.datetime.now().isoformat()
    service_type = random.choice(["快車", "共乘", "尊榮"])

    return {
        "event_type": "ride_request",
        "passenger_id": passenger_id,
        "timestamp": request_time,
        "data": {
            "pickup": {"lat": origin_lat, "lon": origin_lon},
            "dropoff": {"lat": dest_lat, "lon": dest_lon},
            "service": service_type,
            "zone": zone
        }
    }

# Produce events to Kafka topic
def produce_ride_requests(bootstrap_servers="140.119.164.16:9092", topic="ride-requests"):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    print("[Producer] Starting ride request simulation...")
    counter = 0

    try:
        while True:  # Runs forever
            for pid in passenger_pool:
                event = generate_ride_request(pid)
                producer.send(topic, event)
                counter += 1
                # print(f"[Ride Request] #{counter} sent for passenger {pid} ({event['data']['service']}) in {event['data']['zone']}")
                print(json.dumps(event, ensure_ascii=False))
            time.sleep(2)
    finally:
        producer.flush()
        producer.close()
        print(f"[Producer] Finished. Total ride requests sent: {counter}")

if __name__ == "__main__":
    produce_ride_requests()



