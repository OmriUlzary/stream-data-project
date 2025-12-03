import json
import os  # Import os module
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

# Load config from Environment Variable, fallback to default if missing
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'my-kafka.default.svc.cluster.local:9092')
TOPIC = 'wiki-edits'
WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'


def create_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Error connecting to Kafka at {KAFKA_BROKER}: {e}")
        return None


if __name__ == "__main__":
    producer = create_producer()

    if producer:
        print(f"Connected to Kafka at {KAFKA_BROKER}. Streaming to topic '{TOPIC}'...")
        for event in EventSource(WIKI_STREAM_URL):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)
                    if change['type'] == 'edit':
                        producer.send(TOPIC, change)
                except ValueError:
                    pass
    else:
        print("Failed to initialize producer. Exiting.")