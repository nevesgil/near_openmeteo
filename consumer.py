from confluent_kafka import Consumer, KafkaError
from db.db import WeatherData, KafkaOffset, Session  # Assuming you have these models defined
from datetime import datetime
import json

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'weather_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_config)
consumer.subscribe(['weather-topic'])

def process_message(session, message):
    # Parse the message
    data = json.loads(message.value().decode('utf-8'))
    
    # Extract the 'current_weather' section from the message
    current_weather = data.get("current_weather", {})
    
    # Create WeatherData object with the correct data structure
    weather_data = WeatherData(
        timestamp=datetime.now(),
        temperature=current_weather.get('temperature'),
        windspeed=current_weather.get('windspeed'),
        winddirection=current_weather.get('winddirection'),
        weathercode=current_weather.get('weathercode')
    )
    session.add(weather_data)
    session.commit()


def get_last_offset(session, topic, partition):
    return session.query(KafkaOffset).filter_by(
        topic=topic, partition=partition
    ).order_by(KafkaOffset.offset.desc()).first()

def update_offset(session, topic, partition, offset):
    kafka_offset = KafkaOffset(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=datetime.now()
    )
    session.add(kafka_offset)
    session.commit()

def main():
    session = Session()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Check if message has been processed
            last_offset = get_last_offset(session, msg.topic(), msg.partition())
            if last_offset and msg.offset() <= last_offset.offset:
                continue

            # Process the message
            process_message(session, msg)

            # Update the offset
            update_offset(session, msg.topic(), msg.partition(), msg.offset() + 1)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        session.close()

if __name__ == "__main__":
    main()
