import os 

class GetQuery:
    
    def __init__(self):
        pass
    
    def get_source_schema(self):
        return """
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
            'topic' = 'radiation_rawdata',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'weather-consumer-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
    
    def get_sink_schema(self):
        return """
        CREATE TABLE sink_table (
            `Captured Time` STRING,
            avg_latitude DOUBLE,
            avg_longitude DOUBLE,
            avg_value DOUBLE,
            unit STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'radiation_processeddata',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
    
    def get_processing_schema(self):
        return """
        SELECT
            `Captured Time`,
            Latitude AS avg_latitude,
            Longitude AS avg_longitude,
            `Value` AS avg_value,
            `Unit` AS unit
        FROM temp_table
        """