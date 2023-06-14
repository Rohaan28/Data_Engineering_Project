from kafka import KafkaProducer
import json

def send_message(bootstrap_servers, topic, message):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(0, 11, 5),
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    producer.send(topic, value=message)
