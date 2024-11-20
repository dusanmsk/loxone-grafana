#!/usr/bin/python

# Simple script to import Loxone UDP logs into InfluxDB
import os
import socket
import json
import argparse
import re
import logging
from influxdb import InfluxDBClient
from datetime import datetime
from dateutil import tz
# suppress warnings for unverified https request
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============== Configuration =====================
# hostname and port of InfluxDB http API
host = os.getenv('INFLUXDB_HOST', '127.0.0.1')
port = int(os.getenv('INFLUXDB_PORT', 8086))
# use https for connection to InfluxDB
ssl = bool(os.getenv('INFLUXDB_HTTPS', False))
# verify https connection
verify = bool(os.getenv('INFLUXDB_VERIFY_HTTPS', False))
# InfluxDB database name
dbname = os.getenv('INFLUXDB_DATABASE', 'loxone')
# InfluxDB login credentials (optional, specify if you have enabled authentication)
dbuser = os.getenv('INFLUXDB_USER')
dbuser_code = os.getenv('INFLUXDB_PASSWORD')
# local IP and port where the script is listening for UDP packets from Loxone
localIP = os.getenv('BIND_ADDRESS', '0.0.0.0')
localPort = int(os.getenv('BIND_PORT', 2222))


print(f"InfluxDB host: {host}")
print(f"InfluxDB port: {port}")
print(f"Use HTTPS: {ssl}")
print(f"Verify HTTPS: {verify}")
print(f"InfluxDB database name: {dbname}")
print(f"InfluxDB user: {dbuser}")
print(f"InfluxDB password: {dbuser_code}")
print(f"Local IP: {localIP}")
print(f"Local port: {localPort}")

def parse_log_data(data, from_zone, to_zone, debug=False):
    """
    Parse received message
    Syntax: <timestamp>;<measurement_name>;<alias(optional)>:<value>;<tag_1(optional)>;<tag_2(optional)>;<tag_3(optional)>
    Example: "2020-09-10 19:46:20;Bedroom temperature;23.0"
    ** TO DO ** Need to add checks in case something goes wrong
    """
    logging.debug('Received: %s', data)

    end_timestamp = data.find(b';')
    end_name = data.find(b';', end_timestamp+1)
    end_alias = data.find(b':', end_name+1)
    if end_alias < 0:		# -1 means not found
        end_alias = end_name
    end_value = data.find(b';', end_alias+1)

    if end_value < 0:  # ; char not found after value
        end_tag1 = 0
        end_value = len(data)
    else:
        end_tag1 = data.find(b';', end_value + 1)

    if end_tag1 > 0:
        end_tag2 = data.find(b';', end_tag1+1)
    else:
        end_tag2 = 0
    if end_tag2 > 0:
        end_tag3 = data.find(b';', end_tag2+1)
    else:
        end_tag3 = 0
    numeric_const_pattern = r'[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
    rx = re.compile(numeric_const_pattern, re.VERBOSE)

    # Timestamp Extraction
    parsed_data = {'TimeStamp': data[0:end_timestamp]}
    parsed_data['TimeStamp'] = parsed_data['TimeStamp'].replace(b' ', b'T')+b'Z'
    # Timezone conversion to UTC
    local = datetime.strptime(parsed_data['TimeStamp'].decode('utf-8'), b'%Y-%m-%dT%H:%M:%SZ'.decode('utf-8'))
    local = local.replace(tzinfo=from_zone)
    utc = local.astimezone(to_zone)
    parsed_data['TimeStamp'] = utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Name Extraction
    parsed_data['Name'] = data[end_timestamp+1:end_name]

    # Alias Extraction
    if end_alias != end_name:
        parsed_data['Name'] = data[end_name+1:end_alias]

    # Value Extraction
    parsed_data['Value'] = rx.findall(data[end_alias+1:end_value].decode('utf-8'))[0]

    # Tag_1 Extraction
    parsed_data['Tag_1'] = data[end_value+1:end_tag1].rstrip()

    # Tag_2 Extraction
    parsed_data['Tag_2'] = data[end_tag1+1:end_tag2].rstrip()

    # Tag_3 Extraction
    parsed_data['Tag_3'] = data[end_tag2+1:end_tag3].rstrip()

    # Create Json body for Influx
    json_body = [
        {
            "measurement": parsed_data['Name'].decode('utf-8'),
            "tags": {
                "Tag_1": parsed_data['Tag_1'].decode('utf-8'),
                "Tag_2": parsed_data['Tag_2'].decode('utf-8'),
                "Tag_3": parsed_data['Tag_3'].decode('utf-8'),
                "Source": "Loxone",
            },
            "time": parsed_data['TimeStamp'],   # "2009-11-10T23:00:00Z",
            "fields": {
                "value": float(parsed_data['Value'])
            }
        }
    ]

    if debug:
        logging.debug(json.dumps(json_body, indent=2))

    return json_body


def main(host, port, ssl, verify, debug=False):
    f"""Instantiate a connection to the InfluxDB at {host}:{port} and stard listening on UDP port for incoming messages"""

    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
                        format='%(asctime)s %(levelname)s - %(message)s')
    logging.info('Creating InfluxDB client - connection: %s:%s, db: %s', host, port, dbname)

    client = InfluxDBClient(host, port, dbuser, dbuser_code, dbname, ssl, verify)
    client.create_database(dbname)

    # get TZ info
    to_zone = tz.tzutc()
    from_zone = tz.tzlocal()

    # A UDP server
    # Set up a UDP server
    logging.info('Listening for incoming Loxone UDP packets on %s:%s', localIP, localPort)
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Listen on local port
    # (to all IP addresses on this system)
    listen_addr = (localIP, localPort)
    udp_sock.bind(listen_addr)
    logging.debug('Socket attached')

    # Report on all data packets received and
    # where they came from in each case (as this is
    # UDP, each may be from a different source and it's
    # up to the server to sort this out!)
    while True:
        data, addr = udp_sock.recvfrom(1024)
        json_body_log = parse_log_data(data, from_zone, to_zone, debug)
        # Write to influx DB
        client.write_points(json_body_log)


def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        add_help=False, description='Simple Loxone to InfluxDB script')
    parser.add_argument('-h', '--host', type=str, required=False,
                        default=host,
                        help='hostname of InfluxDB http API')
    parser.add_argument('-p', '--port', type=int, required=False, default=port,
                        help='port of InfluxDB http API')
    parser.add_argument('-s', '--ssl', default=ssl, action="store_true",
                        help='use https to connect to InfluxDB')
    parser.add_argument('-v', '--verify', default=verify, action="store_true",
                        help='verify https connection to InfluxDB')
    parser.add_argument('-d', '--debug', action="store_true", default=bool(os.getenv('DEBUG', False)),
                        help='debug code')
    parser.add_argument('-?', '--help', action='help',
                        help='show this help message and exit')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, ssl=args.ssl, verify=args.verify, debug=args.debug)