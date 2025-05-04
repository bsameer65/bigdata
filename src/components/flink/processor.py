import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import TableEnvironment
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"   # or wherever your Kafka is reachable

def create_kafka_topic(topic_name, num_retries=5, retry_delay=5):
    admin_client = None

    for attempt in range(num_retries):
        try:
            print(f"Attempt {attempt+1}: Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

            topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
            break  # Successfully created, exit loop

        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists.")
            break  # No problem, we can continue

        except NoBrokersAvailable as e:
            print(f"No brokers available. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)  # Wait before retrying

        except Exception as e:
            print(f"Error creating topic: {e}")
            time.sleep(retry_delay)

        finally:
            if admin_client:
                admin_client.close()

    else:
        print(f"Failed to connect to Kafka after {num_retries} attempts. Exiting.")
        raise RuntimeError("Could not connect to Kafka broker.")









def main():
    # Create streaming environment
    
    create_kafka_topic('processed_radiation_data')
    # your other main logic...
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
        .in_streaming_mode()\
        .build()

    tbl_env = StreamTableEnvironment.create(env, environment_settings=settings)
    tbl_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///app/flink-sql-connector-kafka-1.17.1.jar"
    #"file:///D:/kafka%20plus%20flink/flink/flink-sql-connector-kafka-1.17.1.jar"
   # "file:///D:/kafka%20plus%20flink/flink-sql-connector-kafka-1.17.1.jar"
)


    src_ddl = """
   CREATE TABLE temp_table (
        `Captured Time` STRING,
        Latitude DOUBLE,
        Longitude DOUBLE,
        `Value` DOUBLE,
        Unit STRING,
        MD5Sum STRING,
        `Uploaded Time` STRING,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'weather',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'weather-consumer-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
"""
    # tbl_env.execute_sql(src_ddl)
    # sink_ddl = """
    #     CREATE TABLE sink_table (
    #         window_end TIMESTAMP(3),
    #         total_measurements BIGINT,
    #         avg_latitude DOUBLE,
    #         avg_longitude DOUBLE
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = 'processed_radiation_data',
    #         'properties.bootstrap.servers' = 'kafka:9092',
    #         'format' = 'json',
    #         'scan.startup.mode' = 'latest-offset',
    #         'json.ignore-parse-errors' = 'true'
    #     )
    # """
    # tbl_env.execute_sql(sink_ddl)
    tbl_env.execute_sql(src_ddl)
    sink_ddl = """
       CREATE TABLE sink_table (
        `Captured Time` STRING,
        avg_latitude DOUBLE,
        avg_longitude DOUBLE,
        avg_value DOUBLE,
        unit STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'processed_radiation_data',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    tbl_env.execute_sql(sink_ddl)


    

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('temp_table')

    print('\nSource Schema')
    tbl.print_schema()

   
    sql = """
         SELECT
        `Captured Time`,
        Latitude AS avg_latitude,
        Longitude AS avg_longitude,
        `Value` AS avg_value,
        `Unit` AS unit
    FROM temp_table



    """
    revenue_tbl = tbl_env.sql_query(sql)
    print("Result Schema:")
    revenue_tbl.print_schema()

    # print('\nProcess Sink Schema')
    # #revenue_tbl.print_schema()
    try:
        print(revenue_tbl.explain())
        revenue_tbl.execute_insert('sink_table').wait()
    except Exception as e:
        print(f"Failed to insert into sink_table: {e}")
    
    print("Data inserted into Sink Table")
    
    

if __name__ == '__main__':
    main()