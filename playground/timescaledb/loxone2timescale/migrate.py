timescaledb_host = 'localhost'
timescaledb_port = '5432'
timescaledb_user = 'loxone'
timescaledb_password = 'loxone'
timescaledb_dbname = 'loxone'
influxdb_host = 'localhost'
influxdb_port = 8086
influxdb_user = 'grafana'
influxdb_password = 'grafana'
influxdb_name = 'loxone'


from enum import unique
from influxdb import InfluxDBClient
import requests
import os
import json
import psycopg2
from psycopg2.extras import execute_values
import concurrent.futures
import threading


num_parallel_migrations = 6
batch_size = 500000

print_lock = threading.Lock()
def tprint(*args):
    with print_lock:
        print(*args)

class ProgressStorage:
    def __init__(self):
        self.progress_file = 'progress.txt'
        self.lock = threading.Lock()
        
    def measurement_done(self, measurement_name):
        with self.lock:
            f = open(self.progress_file, 'a')
            f.write(measurement_name + '\n')
            f.close()

    def get_finished_measurement_names(self):
        try:
            f = open(self.progress_file, 'r')
            lines = f.readlines()
            return set([line.strip() for line in lines])
        except:
            return set()


# name : id
dict_cache = {}
def get_or_create_dict(connection, name):
    global dict_cache
    if not name in dict_cache:
        cursor = connection.cursor()
        cursor.execute(f"""
            INSERT INTO _dictionary (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id;
        """, (name,))
        dict_cache[name] = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
    return dict_cache[name]

# Function to write data to TimescaleDB
def write_to_timescaledb(measurement_name, timescale_data):
    insert_query = """
        INSERT INTO _measurements (timestamp, measurement_name_id, value_name_id, value, value_str) VALUES %s
            ON CONFLICT DO NOTHING;
    """

    timescale_conn2 = psycopg2.connect(
        host=timescaledb_host,
        port=timescaledb_port,
        user=timescaledb_user,
        password=timescaledb_password,
        dbname=timescaledb_dbname
    )
    #timescale_cursor2 = timescale_conn2.cursor()
    
    measurement_name_id = get_or_create_dict(timescale_conn2, measurement_name)
    # transform data to be inserted and add measurement_name_id and value_name_id
    sql_data = []
    for timestamp, value_name, value, value_str in timescale_data:
        sql_data.append((timestamp, measurement_name_id, get_or_create_dict(timescale_conn2, value_name), value, value_str))
    #tprint(f"Inserting data into TimescaleDB measurement {measurement_name}")
    cursor = timescale_conn2.cursor()
    #cursor.execute("SET synchronous_commit TO OFF;")
    execute_values(cursor, insert_query, sql_data, page_size=batch_size)
    timescale_conn2.commit()
    cursor.close()
    timescale_conn2.close()
    #tprint(f"Inserting data into TimescaleDB measurement {measurement_name} done")


def split_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


# connect to TimescaleDB 
timescale_conn = psycopg2.connect(
    host=timescaledb_host,
    port=timescaledb_port,
    user=timescaledb_user,
    password=timescaledb_password,
    dbname=timescaledb_dbname
)
timescale_cursor = timescale_conn.cursor()

tprint("Selecting already migrated measurements...")
progressStorage = ProgressStorage()
#timescale_cursor.execute("SELECT DISTINCT measurement_name FROM testdb.public.loxone_measurements")
#migrated_measurements = set(row[0] for row in timescale_cursor.fetchall())
migrated_measurements = progressStorage.get_finished_measurement_names()

# Get list of measurements
influx_client = InfluxDBClient(
    host=influxdb_host,
    port=influxdb_port,
    username=influxdb_user,
    password=influxdb_password,
    database=influxdb_name
)

tprint("Starting data migration...")
measurements = influx_client.get_list_measurements()
remaining_measurements = len(measurements)

# create list of (timestamp, field_name, field_value, field_value_str)
def convert_to_timescale_data(results):
    timescale_data = []
    for point in results.get_points():
        timestamp = point['time']
        fields = {k: v for k, v in point.items() if k not in ['time', 'measurement']}
        tags = {k: v for k, v in point.items() if k not in fields and k not in ['time', 'measurement']}
        for field_name, field_value in fields.items():
            if isinstance(field_value, (int, float)):
                timescale_data.append((timestamp, field_name, field_value, None))
            else:
                timescale_data.append((timestamp, field_name, None, field_value))
    return timescale_data

# Function to migrate a measurement
def migrate_measurement(measurement):
    try:
        global remaining_measurements
        measurement_name = measurement['name']
        if measurement_name in migrated_measurements:
            tprint(f"Measurement {measurement_name} already migrated. Skipping...")
            remaining_measurements = remaining_measurements - 1
            return
        influx_client2 = InfluxDBClient(
            host=influxdb_host,
            port=influxdb_port,
            username=influxdb_user,
            password=influxdb_password,
            database=influxdb_name
        )
        start_offset = 0
        while True:
            tprint("Migrating data for measurement:", measurement_name)
            query = f'SELECT * FROM "{measurement_name}" LIMIT {batch_size} OFFSET {start_offset}'
            # tprint(f"   Executing query: {query}")
            results = influx_client2.query(query)
            timescale_data = convert_to_timescale_data(results)
            if(len(timescale_data) == 0):
                break
            write_to_timescaledb(measurement_name, timescale_data)
            start_offset += batch_size

        remaining_measurements = remaining_measurements - 1
        tprint(f"Data migration completed for measurement: {measurement_name}. Remaining measurements: {remaining_measurements}")
        results=None
        influx_client2.close()
        progressStorage.measurement_done(measurement_name)

    except Exception as e:
        tprint(f"Error migrating data for measurement: {measurement_name}. Error: {e}")
        
# Create a ThreadPoolExecutor with the specified number of parallel migrations
with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallel_migrations) as executor:
    # Submit the migration tasks to the executor
    migration_tasks = [executor.submit(migrate_measurement, measurement) for measurement in measurements]

    # Wait for all migration tasks to complete
    concurrent.futures.wait(migration_tasks)


# kompresia historickych dat
timescale_cursor.execute("""
ALTER TABLE loxone_measurements SET (timescaledb.compress, timescaledb.compress_segmentby = 'measurement_name');
SELECT add_compression_policy('loxone_measurements', INTERVAL '120 days');
""")
timescale_conn.commit()

# Close TimescaleDB connection
timescale_cursor.close()
timescale_conn.close()
influx_client.close()
tprint("Data migration completed.")

