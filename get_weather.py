import requests
import json
import time
from kafka import KafkaProducer

# OpenWeatherMap API 設定
API_KEY = '165d62a3bd069c9c0b6be954a8810c66'  # 天氣 API 金鑰
LAT = 25.032435  # 大安森林公園緯度
LON = 121.534905  # 大安森林公園經度
LOCATION_NAME = "Taipei"
URL = f'https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric&lang=zh_tw'

# Kafka Producer 設定
producer = KafkaProducer(
    bootstrap_servers='140.119.164.16:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def send_weather():
    try:
        response = requests.get(URL)
        data = response.json()
        weather_event = {
            "event_type": "weather_update",
            "location": LOCATION_NAME,
            "timestamp": data.get("dt"),
            "data": {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"]
            }
        }
        producer.send("weather-data", weather_event)
        print(f"已發送天氣資料：{weather_event}")
    except Exception as e:
        print(f"取得天氣資料時發生錯誤：{e}")

# 每 5 分鐘取得一次天氣資料
if __name__ == "__main__":
    while True:
        send_weather()
        time.sleep(5)  # 300 秒 = 5 分鐘