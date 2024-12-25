import requests
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:29092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


for _ in range(100): 
    response = requests.get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": 23.5,
        "longitude": 46.625,
        "current_weather": "true"
    })
    weather_data = response.json()

    producer.produce('test_topic', value=json.dumps(weather_data), callback=delivery_report)

    producer.poll(0)

producer.flush()
