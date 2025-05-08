from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import os
#from components import logger
import time

class KafkaTopic:
    def __init__(self):
        self.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" 
        self.destination_topic = "radiation_processeddata"
        self.total_retries = 5
        self.delay = 5
        


    def create_kafka_topic(self):
        admin_client = None

        for retry in range(self.total_retries):
            try:
                print(f"Connecting to Kafka server at {self.KAFKA_BOOTSTRAP_SERVERS} : Attempt {retry+1}...")
                #logger.info(f"Creating destination Topic--> Connecting to Kafka server at {self.KAFKA_BOOTSTRAP_SERVERS} : Attempt {retry+1}...")
                admin_client = KafkaAdminClient(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS)

                topic_list = [NewTopic(name=self.destination_topic, num_partitions=1, replication_factor=1)]
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                
                print(f"Successfully created '{self.destination_topic}' Topic.")
                #logger.info(f"Successfully created '{self.destination_topic}' Topic.")
                break 
    

            except TopicAlreadyExistsError:
                print(f"Topic '{self.destination_topic}' already exists.")
                #logger.info(f"Topic '{self.destination_topic}' already exists.")
                break  

            except NoBrokersAvailable as e:
                print(f"No brokers available. Retrying in {self.delay} seconds...")
                #logger.info(f"No brokers available. Retrying in {self.delay} seconds...")
                time.sleep(self.delay)  

            except Exception as e:
                print(f"Error creating topic: {e}")
                #logger.info(f"Error creating topic: {e}")
                time.sleep(self.delay)

            finally:
                if admin_client:
                    admin_client.close()

        else:
            print(f"Failed to connect to Kafka after {self.total_retries} attempts. Exiting.")
            #logger.info(f"Failed to connect to Kafka after {self.total_retries} attempts. Exiting.")
            raise RuntimeError("Could not connect to Kafka broker.")







