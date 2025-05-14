from confluent_kafka import Consumer, KafkaException, KafkaError
import asyncio
from fastapi import WebSocket
import json

# Kafka Consumer setup
consumer_config = {
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
    'group.id': 'websocket_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start from the earliest offset
}

consumer = Consumer(consumer_config)

# WebSocket client connections
connected_clients = set()

async def broadcast_to_clients(message: dict):
    """Send the Kafka message to all connected WebSocket clients"""
    for client in connected_clients:
        await client.send_text(json.dumps(message))

async def consume_kafka_topic():
    consumer.subscribe(['processed_radiation_data'])  # Subscribe to the topic

    try:
        while True:
            # Poll Kafka for new messages
            msg = consumer.poll(timeout=1.0)  # Adjust the timeout as necessary

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached: {}'.format(msg))
                else:
                    raise KafkaException(msg.error())
            else:
                # Successfully received a message, broadcast it to WebSocket clients
                message = msg.value().decode('utf-8')
                print(f"Received message from Kafka: {message}")
                await broadcast_to_clients(json.loads(message))  # Send to WebSocket clients

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Start Kafka consumer in the background
async def start_kafka_consumer():
    await consume_kafka_topic()

