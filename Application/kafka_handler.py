from kafka import KafkaProducer
import json

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "Topic_1_Trade_Data"

# Create Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def send_data_to_kafka_topic():
    # Send data to Kafka topic
    kafka_producer.send(kafka_topic, value="Data received".encode('utf-8'))
