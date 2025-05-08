import os
import time
import threading
import pandas as pd
from kafka import KafkaProducer as KProducer
from kafka.errors import NoBrokersAvailable
from json import dumps
#from src.components import logger


class KafkaProducerHandler:
    def __init__(self):
        self.kafka_conn_nodes = os.getenv('KAFKA_SERVER', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'radiation_rawdata')
        self.input_file = os.getenv('INPUT_FILE', '/data/measurements.csv')
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 1000000))
        self.delay = int(os.getenv('RETRY_DELAY', 5))
        self.total_producers = int(os.getenv('TOTAL_PRODUCERS', 5))
        self.total_retry = int(os.getenv('TOTAL_RETRIES', 10))

    def create_producer(self, producer_id):
        """Create a Kafka producer instance with retries."""
        for retry in range(self.total_retry):
            try:
                #logger.info(f"Producer {producer_id}: Attempting Kafka connection (try {retry+1})...")
                producer = KProducer(
                    bootstrap_servers=self.kafka_conn_nodes,
                    value_serializer=lambda x: dumps(x).encode('utf-8'),
                )
                #logger.info(f"Producer {producer_id}: Connected successfully to Kafka.")
                return producer
            except NoBrokersAvailable as e:
                #logger.warning(f"Producer {producer_id}: Kafka broker not available, retrying in {self.delay*(2**retry)} seconds... ({retry+1}/{self.total_retry})")
                time.sleep(self.delay * (2 ** retry))  # exponential backoff
        #logger.error(f"Producer {producer_id}: Failed to connect to Kafka after {self.total_retry} retries.")
        raise Exception(f"Kafka connection failed for producer {producer_id}")

    def send_data(self, producer, data, producer_id):
        """Sending data to Kafka Topic."""
        row_count = 0
        for _, row in data.iterrows():
            df_json = row.to_dict()
            if row_count == 0:
                print("Producer {producer_id}: Sending first row {df_json}")
                #logger.info(f"Producer {producer_id}: Sending first row {df_json}")
            producer.send(topic=self.topic, value=df_json)
            row_count += 1
        producer.flush()
        #logger.info(f"Producer {producer_id}: Successfully sent {row_count} records.")
        print(f"Producer {producer_id} sent {row_count} records.")

    def pipeline(self, producer_id, data):
        """Launching producer and sending data Pipeline."""
        try:
            producer = self.create_producer(producer_id)
            self.send_data(producer, data, producer_id)
            producer.close()
        except Exception as e:
            print("Producer {producer_id}: Failed during pipeline. Error: {e}")
            #logger.exception(f"Producer {producer_id}: Failed during pipeline. Error: {e}")

    def launch_producers(self, data_chunks):
        """Creating multiple producer threads."""
        threads = []
        for i in range(self.total_producers):
            thread = threading.Thread(target=self.pipeline, args=(i, data_chunks[i]))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def process_file(self):
        """Main pipeline to process the CSV file and send to Kafka."""
        columns = ['Captured Time', 'Latitude', 'Longitude', 'Value', 'Unit', 'MD5Sum', 'Uploaded Time']
        try:
            radiation_data = pd.read_csv(self.input_file, chunksize=self.chunk_size, usecols=columns)
            for idx, chunk in enumerate(radiation_data):
                print(f"Processing chunk {idx+1}...")
                chunk = chunk.dropna(subset=['Captured Time']).sort_values('Captured Time')
                chunk_division = [chunk.iloc[i::self.total_producers] for i in range(self.total_producers)]
                self.launch_producers(chunk_division)
                print(f"inished processing chunk {idx+1}")
            print("All data sent to Kafka successfully!")
        except Exception as e:
            #logger.exception(f"Error processing file: {e}")
            print(f"Error during processing: {e}")


if __name__ == "__main__":
    kafka_handler = KafkaProducerHandler()
    kafka_handler.process_file()
