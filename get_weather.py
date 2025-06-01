import requests
import json
import time
from kafka import KafkaProducer

# OpenWeatherMap API 
API_KEY = '165d62a3bd069c9c0b6be954a8810c66'  
URL_TEMPLATE = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric&lang=zh_tw'

# 台北市各區中心經緯度
district_coords = {
    '中正區': (25.0324, 121.5186),
    '大同區': (25.0630, 121.5152),
    '中山區': (25.0685, 121.5266),
    '松山區': (25.0504, 121.5572),
    '大安區': (25.032435, 121.534905),  
    '萬華區': (25.0330, 121.4970),
    '信義區': (25.0338, 121.5623),
    '士林區': (25.0954, 121.5240),
    '北投區': (25.1316, 121.5066),
    '內湖區': (25.0830, 121.5805),
    '南港區': (25.0416, 121.6135),
    '文山區': (24.9884, 121.5680)
}

# Kafka Producer 設定
producer = KafkaProducer(
    bootstrap_servers='140.119.164.16:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def send_weather_all_districts():
    for district, (lat, lon) in district_coords.items():
        url = URL_TEMPLATE.format(lat=lat, lon=lon, key=API_KEY)
        try:
            response = requests.get(url)
            data = response.json()

            weather_event = {
                "event_type": "weather_update",
                "location": district,
                "timestamp_api": data.get("dt"),  #
                "timestamp_produced": int(time.time()),  
                "data": {
                    "temperature": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "weather": data["weather"][0]["description"]
                }
            }

            producer.send("weather-data", weather_event)
            print(f"已發送：{district} → {weather_event}")

        except Exception as e:
            print(f"發送失敗：{district} → {e}")

# 6 min get weather data 
if __name__ == "__main__":
    while True:
        send_weather_all_districts()
        time.sleep(600)  #6 min