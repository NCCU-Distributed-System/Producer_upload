import json
import random
import time
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='140.119.164.16:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# random交通狀況資料
def generate_traffic_event():
    locations = ["台北市中正區", "台北市信義區", "台北市大安區"]
    location = random.choice(locations)
    speed = random.uniform(10, 70)  # random生成車速
    if speed > 50:
        status = "順暢"
    elif speed > 30:
        status = "中等"
    else:
        status = "擁塞"
    event = {
        "event_type": "traffic_update",
        "location": location,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data": {
            "speed": round(speed, 2),
            "status": status
        }
    }
    return event

# 發送交通狀況到 Kafka
def send_traffic_data():
    event = generate_traffic_event()
    producer.send("traffic-data", event)
    print(f"已發送交通資料：{event}")


if __name__ == "__main__":
    while True:
        send_traffic_data()
        time.sleep(2)  #時間設定
