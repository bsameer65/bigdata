import os
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import TableEnvironment
from create_topic import KafkaTopic
from get_queries import GetQuery


class FlinkProcessor:
    def __init__(self):
        self.dest_topic = KafkaTopic()
        self.queries = GetQuery()

    def configuration(self):
        get_env = StreamExecutionEnvironment.get_execution_environment()

        env_settings = EnvironmentSettings.new_instance()\
            .in_streaming_mode()\
            .build()

        conn = StreamTableEnvironment.create(get_env, environment_settings=env_settings)
        conn.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///app/flink-sql-connector-kafka-1.17.1.jar")
        
        return conn
        
    def processing_pipeline(self):

        self.dest_topic.create_kafka_topic()
        conn = self.configuration()

        source_table = self.queries.get_source_schema()
        sink_table = self.queries.get_sink_schema()
        
        conn.execute_sql(source_table)
        conn.execute_sql(sink_table)

        # create and initiate loading of source Table
        src_schema = conn.from_path('temp_table')

        print('\nSource Schema')
        src_schema.print_schema()

    
        data_processing = self.queries.get_processing_schema()
        sink_schema = conn.sql_query(data_processing)
        print("Result Schema:")
        sink_schema.print_schema()

        # print('\nProcess Sink Schema')
        # #revenue_tbl.print_schema()
        try:
            #print(sink_schema.explain())
            sink_schema.execute_insert('sink_table').wait()

        except Exception as e:
            print(f"Failed to insert into sink_table: {e}")
        
        print("Data inserted into Sink Table")
        
        

if __name__ == '__main__':
    flink_processor = FlinkProcessor()
    flink_processor.processing_pipeline()


