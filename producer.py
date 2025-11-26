import json
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'my-kafka.default.svc.cluster.local:9092' # K8s DNS
TOPIC = 'wiki-edits'
WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

if __name__ == "__main__":
    producer = create_producer()
    print(f"Connected to Kafka at {KAFKA_BROKER}. Streaming to topic '{TOPIC}'...")

    # Consume from Wikimedia (Server-Sent Events)
    for event in EventSource(WIKI_STREAM_URL):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                # Filter for edits only (optional)
                if change['type'] == 'edit':
                    producer.send(TOPIC, change)
            except ValueError:
                pass