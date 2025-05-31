import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='140.119.164.16:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

districts = ['中正區', '大同區', '中山區', '松山區', '大安區', '萬華區',
             '信義區', '士林區', '北投區', '內湖區', '南港區', '文山區']

def generate_traffic_event(location):
    hour = datetime.now().hour
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        status = random.choices(["擁塞", "中等", "順暢"], weights=[0.70, 0.20, 0.10])[0]
    else:
        status = random.choices(["擁塞", "中等", "順暢"], weights=[0.1, 0.3, 0.6])[0]

    if status == "擁塞":
        speed = random.randint(1, 39)
    elif status == "中等":
        speed = random.randint(40, 60)
    else:
        speed = random.randint(61, 90)

    event = {
        "event_type": "traffic_update",
        "location": f"台北市{location}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": {
            "speed": speed,
            "status": status
        }
    }
    return event

topic_name = "traffic-data"
event_count = 0

try:
    while True:
        all_events = []
        for district in districts:
            event = generate_traffic_event(district)
            all_events.append(event)
        
        all_events_json = json.dumps(all_events, ensure_ascii=False, indent=2)
        producer.send(topic_name, all_events_json)
        event_count += 1

        print(f"已發送第 {event_count} 筆交通資料包，共 {len(all_events)} 筆事件。")
        for event in all_events:
            print(f"已發送交通資料：{event}")

        time.sleep(random.uniform(0.5, 5.0))

except KeyboardInterrupt:
    print("手動中止資料產生。")

finally:
    producer.flush()
    producer.close()
    print(f"資料傳送結束，共發送 {event_count} 筆交通資料包。")
