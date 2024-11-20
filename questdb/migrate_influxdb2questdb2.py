import argparse
import concurrent.futures
import datetime
import logging
import os
import re
import signal
import sys

import requests
from influxdb import InfluxDBClient
from questdb.ingress import TimestampNanos
from tqdm import tqdm
import questdb.ingress

parallel_jobs = int(os.cpu_count() / 2)
batch_size = 10000

# Nastavenie InfluxDB klienta
influx_host = "localhost"
influx_port = 8086
influx_db = "loxone"
influx_user = "loxone"  # Nastavte, ak máte používateľa
influx_password = "loxone"  # Nastavte, ak máte heslo

questdb_host = "localhost"
questdb_port = 9000
questdb_username = "admin"
questdb_password = "quest"
questdb_tablename_prefix = ""
auto_flush_rows = 1000
auto_flush_interval = 300000

main_progressbar = None
do_shutdown = False

class InfluxDBClientContextManager:
    def __init__(self, *args, **kwargs):
        self.client = InfluxDBClient(*args, **kwargs)
        self.client.switch_database(influx_db)

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

def create_influx_client():
    global influx_host, influx_port, influx_user, influx_password, influx_db
    return InfluxDBClientContextManager(host=influx_host, port=influx_port, username=influx_user, password=influx_password)

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
            return 0


def get_questdb_oldest_timestamp(measurement):
    query = f"SELECT min(timestamp) FROM {measurement}"
    url = f"http://{questdb_host}:{questdb_port}/exec"
    response = requests.get(url, params={"query": query})
    date = datetime.datetime.now()
    if response.status_code == 200:
        results = response.json()
        date_str = results['dataset'][0][0]
        date = parse_timestamp(date_str)
    return date


def to_epoch(dt):
    return int(dt.timestamp() * 1_000_000_000)

def parse_timestamp(ts):
    date = None
    if '.' in ts:
        date = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        date = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    return date.replace(tzinfo=datetime.timezone.utc)

def split_into_chunks(array, chunk_size):
    return [array[i:i + chunk_size] for i in range(0, len(array), chunk_size)]


conf = f'http::addr={questdb_host}:{questdb_port};username={questdb_username};password={questdb_password};auto_flush_rows={auto_flush_rows};auto_flush_interval={auto_flush_interval};'
def insert_chunk_into_questdb(measurement_name, chunk):
    try:
        global questdb_tablename_prefix
        table_name = f"{questdb_tablename_prefix}{measurement_name}"
        with questdb.ingress.Sender.from_conf(conf) as sender:
            for row in chunk:
                ts = row['time']
                del row['time']
                sender.row(
                    table_name,
                    # convert all numeric values to float
                    columns={k: float(v) if isinstance(v, (int, float)) else v for k, v in row.items()},
                    at=TimestampNanos(ts)
                )
            sender.flush()
    except Exception as e:
        logging.error(f"Failed to insert chunk into QuestDB: {e}")


def insert_to_questdb(measurement_name, data):
    num_parallalel = 5
    chunks = split_into_chunks(data, int(batch_size/num_parallalel))
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallalel) as executor:
        futures = [executor.submit(insert_chunk_into_questdb, measurement_name, chunk) for chunk in chunks]
        concurrent.futures.wait(futures)



def do_export(measurement):
    with create_influx_client() as influx_client:
        try:
            lowest_timestamp = to_epoch(get_questdb_oldest_timestamp(measurement))
            influx_time_where = f"time < {lowest_timestamp}"
            total_rows = get_influx_count(measurement, influx_time_where)
            pbar = tqdm(total=total_rows, desc=measurement, leave=False)
            while True:
                global do_shutdown
                if do_shutdown:
                    break
                query = f"SELECT * FROM {measurement} WHERE time < {lowest_timestamp} ORDER BY time DESC LIMIT {batch_size}"
                results = influx_client.query(query, epoch='ns')
                points = list(results.get_points())
                if not points:
                    break
                lowest_timestamp = points[-1]['time']
                insert_to_questdb(measurement, points)
                pbar.update(len(points))
            pbar.close()

        except Exception as e:
            # todo log to file
            print(f"Failed to export {measurement}: {e}")

        finally:
            global main_progressbar
            main_progressbar.update(1)


def main():
    parser = argparse.ArgumentParser(description='Migrate InfluxDB to QuestDB')
    parser.add_argument('-d', help='Database name')
    parser.add_argument('-m', help='Measurement name', required=False, action='append')
    parser.add_argument('-r', help='Measurement name regex', required=False, action='append')
    parser.add_argument('-e', help='Exclude measurement name regex', required=False, action='append')
    parser.add_argument('-p', help='Prefix for questdb table names', required=False)
    args = parser.parse_args()


    if args.d:
        global influx_db
        influx_db = args.d

    if args.p:
        global questdb_tablename_prefix
        questdb_tablename_prefix = args.p

    measurements = []
    if args.m:
        measurements = [args.m]
    else:
        with create_influx_client() as influx_client:
            measurements = sorted([m['name'] for m in influx_client.get_list_measurements()])
    if args.r:
        for include_pattern in args.p:
            measurements = [m for m in measurements if re.match(include_pattern, m)]

    if args.e:
        for exclude_pattern in args.e:
            measurements = [m for m in measurements if not re.match(exclude_pattern, m)]

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
