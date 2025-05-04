
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import threading
import time
from kafka.errors import NoBrokersAvailable
import os

# Kafka settings
kafka_nodes = os.getenv('KAFKA_SERVER', 'kafka:9092')  # Use environment variable for Kafka server address
myTopic = "weather"
chunk_size = 10000  # Size of each chunk to process

def create_kafka_producer(producer_id, max_retries=10, delay=5):
    """Try to connect to Kafka with retries."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_nodes,
                value_serializer=lambda x: dumps(x).encode('utf-8'),
            )
            print(f"✅ Kafka producer {producer_id} connected")
            return producer
        except NoBrokersAvailable:
            print(f"❌ Kafka not available for producer {producer_id}, retrying in {delay} seconds... ({attempt+1}/{max_retries})")
            time.sleep(delay)
    raise Exception(f"Kafka not available after retries for producer {producer_id}")


def gen_data(producer, chunk, producer_id):
    i = 0
    for _, row in chunk.iterrows():
        df_json = row.to_dict()
        if i == 0:
            print(f"Producer {producer_id} sending: {df_json}")
        i += 1
        producer.send(topic=myTopic, value=df_json)
    producer.flush()
    print(f"Producer {producer_id} sent {i} rows successfully")


def start_producer(producer_id, data_chunk):
    producer = create_kafka_producer(producer_id)
    gen_data(producer, data_chunk, producer_id)

if __name__ == "__main__":

    data_file_path = '/data/measurements.csv'  # Path inside the container
    data = pd.read_csv(data_file_path,nrows=20)  # Read the CSV file data[['Captured Time', 'Latitude', 'Longitude', 'Value', 'Unit', 'MD5Sum', 'Uploaded Time']]
    data_copy = data[['Captured Time', 'Latitude', 'Longitude']].dropna()
    producers = []

    num_producers = 5  
    chunks = [data_copy.iloc[i::num_producers] for i in range(num_producers)]

    for i in range(num_producers):
        producer_thread = threading.Thread(target=start_producer, args=(i, chunks[i]))
        producers.append(producer_thread)
        producer_thread.start()

    for producer_thread in producers:
        producer_thread.join()

