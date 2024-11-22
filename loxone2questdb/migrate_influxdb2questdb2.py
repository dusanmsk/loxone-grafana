from lib import *
import argparse
import concurrent.futures
import datetime
import logging
import os
import re
import signal
import sys
import traceback
import requests
from influxdb import InfluxDBClient
from questdb.ingress import TimestampNanos
from tqdm import tqdm

def get_env_var(name):
    value = os.environ.get(name)
    assert value, f"{name} environment variable is not set."
    return value

questdb_host = get_env_var('QUESTDB_HOST')
questdb_port = get_env_var('QUESTDB_PORT')
questdb_username = get_env_var('QUESTDB_USERNAME')
questdb_password = get_env_var('QUESTDB_PASSWORD')

influxdb_host = get_env_var('INFLUXDB_HOST')
influxdb_port = int(get_env_var('INFLUXDB_PORT'))
influxdb_name = get_env_var('INFLUXDB_NAME')
influxdb_user = get_env_var('INFLUXDB_USER')
influxdb_password = get_env_var('INFLUXDB_PASSWORD')

auto_flush_rows = 1000
auto_flush_interval = 300000
parallel_jobs = int(os.cpu_count() / 2)
batch_size = 10000
max_chunks = None

main_progressbar = None
do_shutdown = False
skip_errors = False
questdb_tablename_prefix = ""

measurements = []

def createQuestDbUtil():
    return QuestDbUtil(questdb_host, questdb_port, questdb_username, questdb_password, auto_flush_rows, auto_flush_interval)

class InfluxDBClientContextManager:
    def __init__(self, *args, **kwargs):
        self.client = InfluxDBClient(*args, **kwargs)
        self.client.switch_database(influxdb_name)

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


def create_influx_client():
    global influxdb_host, influxdb_port, influxdb_user, influxdb_password, influxdb_name
    return InfluxDBClientContextManager(host=influxdb_host, port=influxdb_port, username=influxdb_user, password=influxdb_password)


def get_influx_count(measurement, where=None):
    with create_influx_client() as influx_client:
        try:
            query = f"SELECT COUNT(*) FROM {measurement}" + f" WHERE {where}" if where else ""
            results = influx_client.query(query)
            points = list(results.get_points())
            if not points:
                return 0
            integer_values = [value for value in points[0].values() if isinstance(value, int)]
            max_value = max(integer_values, default=None)
            return max_value
        except Exception as e:
            print(f"Failed to get count for {measurement}: {e}")
            traceback.print_exc()
            return 0

def get_influxdb_field_types(measurement):
    field_types = {}
    with create_influx_client() as influx_client:
        query = f"SHOW FIELD KEYS FROM {measurement}"
        results = influx_client.query(query)
        for point in results.get_points():
            field_types[point['fieldKey']] = point['fieldType']
    return field_types

def to_epoch(dt):
    return int(dt.timestamp() * 1_000_000_000)

def split_into_chunks(array, chunk_size):
    return [array[i:i + chunk_size] for i in range(0, len(array), chunk_size)]


def exitOnError():
    global skip_errors
    global do_shutdown
    if not skip_errors:
        do_shutdown = True
        sys.exit(1)

def type_match(influx_type, questdb_type):
    if questdb_type is None:        # column doesnot exists in db so will be created automatically
        return True
    if influx_type == "string":
        return questdb_type == "SYMBOL"
    if influx_type == "boolean":
        return questdb_type == "BOOLEAN"
    if influx_type == "integer":
        return questdb_type == "DOUBLE"
    if influx_type == "float":
        return questdb_type == "DOUBLE"
    if influx_type == "unsigned":
        return questdb_type == "DOUBLE"
    if influx_type == "long":
        return questdb_type == "DOUBLE"
    if influx_type == "double":
        return questdb_type == "DOUBLE"
    if influx_type == "dateTime":
        return questdb_type == "TIMESTAMP"
    return False


def convertField(key, value, influx_type, questdb_type):
    if questdb_type in ["SYMBOL", "STRING", "VARCHAR"]:
        return key, str(value)
    if questdb_type in ["DOUBLE", "FLOAT"]:
        try:
            return key, float(value)
        except ValueError:
            return f"{key}_str", str(value)

def convertTypes(fields, influx_field_types, questdb_field_types):
    columns = {}
    for key, value in fields.items():
        influx_type = influx_field_types.get(key)
        questdb_type = questdb_field_types.get(key)
        if(type_match(influx_type, questdb_type)):
            columns[key] = value
        else:
            key, value = convertField(key, value, influx_type, questdb_type)
            columns[key] = value
    return columns


def insert_chunk_into_questdb(measurement_name, chunk, influx_field_types, questdb_field_types):
    try:
        global questdb_tablename_prefix
        table_name = f"{questdb_tablename_prefix}{measurement_name}"
        with QuestDbUtil(questdb_host, questdb_port, questdb_username, questdb_password, auto_flush_rows, auto_flush_interval) as sender:
            for row in chunk:
                ts = row['time']
                del row['time']
                columns = convertTypes(row, influx_field_types, questdb_field_types)
                sender.insert_to_questdb(table_name, columns, TimestampNanos(ts))
    except Exception as e:
        logging.error(f"Failed to insert chunk into QuestDB: {e}")
        traceback.print_exc()
        exitOnError()


def insert_to_questdb(measurement_name, data, influxdb_field_types, questdb_field_types):
    num_parallalel = 5
    chunks = split_into_chunks(data, int(batch_size / num_parallalel))
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallalel) as executor:
        futures = [executor.submit(insert_chunk_into_questdb, measurement_name, chunk, influxdb_field_types, questdb_field_types) for chunk in chunks]
        concurrent.futures.wait(futures)


def do_export(measurement):
    with create_influx_client() as influx_client:
        with createQuestDbUtil() as questdb_util:
            try:
                lowest_timestamp = to_epoch(questdb_util.get_questdb_oldest_timestamp(measurement))
                influx_time_where = f"time < {lowest_timestamp}"
                total_rows = get_influx_count(measurement, influx_time_where)
                pbar = tqdm(total=total_rows, desc=measurement, leave=False)
                influx_field_types = get_influxdb_field_types(measurement)
                questdb_field_types = questdb_util.getQuestDbTableColumnTypes(measurement)
                chunk_cnt = 0
                while True:
                    global do_shutdown, max_chunks
                    if do_shutdown:
                        break
                    query = f"SELECT * FROM {measurement} WHERE time < {lowest_timestamp} ORDER BY time DESC LIMIT {batch_size}"
                    results = influx_client.query(query, epoch='ns')
                    points = list(results.get_points())
                    if not points:
                        break
                    lowest_timestamp = points[-1]['time']
                    insert_to_questdb(measurement, points, influx_field_types, questdb_field_types)
                    pbar.update(len(points))
                    chunk_cnt += 1
                    if max_chunks != None and chunk_cnt >= max_chunks:
                        break
                pbar.close()

            except Exception as e:
                # todo log to file
                print(f"Failed to export {measurement}: {e}")
                traceback.print_exc()
                exitOnError()

            finally:
                global main_progressbar
                main_progressbar.update(1)


def processArgs():
    global measurements, influxdb_name, influxdb_host, influxdb_port, influxdb_user, influxdb_password, questdb_host, questdb_port, questdb_username, questdb_password, parallel_jobs, skip_errors, max_chunks, questdb_tablename_prefix
    parser = argparse.ArgumentParser(description='Migrate InfluxDB to QuestDB')
    parser.add_argument('-d', help='Database name')
    parser.add_argument('-i', help='InfluxDB connection string (host:port:user:password:database_name)', required=True)
    parser.add_argument('-q', help='QuestDB connection string (host:port:user:password)', required=True)
    parser.add_argument('-m', help='Measurement name', required=False, action='append')
    parser.add_argument('-r', help='Measurement name regex', required=False, action='append')
    parser.add_argument('-e', help='Exclude measurement name regex', required=False, action='append')
    parser.add_argument('-p', help='Prefix for questdb table names', required=False)
    parser.add_argument('-j', help='Number of parallel jobs', required=False, default=os.cpu_count())
    parser.add_argument('-s', help='Skip errors', required=False, default=False, action='store_true')
    parser.add_argument('-c', help='Process only specified number of chunks', required=False)
    args = parser.parse_args()

    if args.i:
        influxdb_host, influxdb_port, influxdb_user, influxdb_password, influxdb_name = args.i.split(':')

    if args.q:
        questdb_host, questdb_port, questdb_username, questdb_password = args.q.split(':')

    if args.d:
        influxdb_name = args.d

    if args.p:
        questdb_tablename_prefix = args.p

    measurements = []
    if args.m:
        measurements = [a for a in args.m]
    else:
        with create_influx_client() as influx_client:
            measurements = sorted([m['name'] for m in influx_client.get_list_measurements()])
    if args.r:
        for include_pattern in args.r:
            measurements = [m for m in measurements if re.match(include_pattern, m)]

    if args.e:
        for exclude_pattern in args.e:
            measurements = [m for m in measurements if not re.match(exclude_pattern, m)]

    if args.j:
        parallel_jobs = int(args.j)

    if args.s:
        skip_errors = args.s

    if args.c:
        max_chunks = int(args.c)


def main():
    processArgs()
    global main_progressbar
    main_progressbar = tqdm(total=len(measurements), desc="Total Progress", position=0, leave=True)

    # parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as executor:
        def signal_handler(sig, frame):
            global do_shutdown
            do_shutdown = True
            executor.shutdown(wait=False, cancel_futures=True)
            print("\nMigration interrupted")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        futures = [executor.submit(do_export, m) for m in measurements]
        concurrent.futures.wait(futures)

    print("Migration done")


if __name__ == "__main__":
    main()
