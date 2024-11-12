from datetime import timedelta
import logging
import threading, datetime
from str2bool import str2bool
import os, sys
from time import sleep
import paho.mqtt.client as mqtt
import json
import re
import psycopg2

flush_interval=10

err_cnt = 0
processed_cnt = 0
timescale_connection = None
verbose = False
timescale_cache = []
last_flush_time = datetime.datetime.now()

logging.info("Starting loxone2timescale")

def get_env_var(name):
    value = os.environ.get(name)
    assert value, f"{name} environment variable is not set."
    return value

def flush_cache():
    global timescale_cache
    logging.info("flush1")
    if timescale_cache:
        cursor = timescale_connection.cursor()
        for timestamp, measurement_name, value_name, value in timescale_cache:
            if isinstance(value, (int, float)):
                cursor.execute("""
                    INSERT INTO loxone_measurements (timestamp, measurement_name, value_name, value, value_str)
                    VALUES (%s, %s, %s, %s, NULL)
                """, (timestamp, measurement_name, value_name, value))
            else:
                cursor.execute("""
                    INSERT INTO loxone_measurements (timestamp, measurement_name, value_name, value, value_str)
                    VALUES (%s, %s, %s, NULL, %s)
                """, (timestamp, measurement_name, value_name, value))
        timescale_connection.commit()
        cursor.close()
        timescale_cache.clear()


def insert_to_timescaledb(measurement_name, value_name, value):
    global last_flush_time
    now = datetime.datetime.now()
    timescale_cache.append((now, measurement_name, value_name, value))
    if last_flush_time + timedelta(seconds=flush_interval) < now:
        flush_cache()
        last_flush_time = now

# prevedie hodnotu na cislo alebo string. U hodnot kde je cislo a string (napriklad "1.0 kW") sa pokusi extrahovat 1.0 ako cislo
def fix_value(value):
    if isinstance(value, (int, float)):
        return value
    else:
        s = str(value)
        if s.lower() in ["true", "on", "zap", "ano", "yes", "1"]:
            return 1
        elif s.lower() in ["false", "off", "vyp", "ne", "no", "0"]:
            return 1
        if " " in s:
            words = s.split(" ")
            s = words[0].strip()
        # odzadu odmazava znaky, kym nenarazi na cislo (odreze kW, W, %, atd.)
        while s and not s[-1].isdigit():
            s = s[:-1]
        try:
            return float(s)
        except ValueError:
            return str(s)


def get_measurement_name(topic, loxone_mqtt_topic_name):
    s = topic.replace(f"{loxone_mqtt_topic_name}/", "").replace("/state", "")
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    while "__" in s:
        s = s.replace("__", "_")
    if s.endswith("_"):
        s = s[:-1]
    return s


def mqtt_on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        # Subscribe to the LOXONE_MQTT_TOPIC_NAME
        client.subscribe(f"{loxone_mqtt_topic_name}/#")
    else:
        logging.error("Failed to connect to MQTT broker")
        sys.exit(1)

def mqtt_on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        measurement_name = get_measurement_name(msg.topic, loxone_mqtt_topic_name)
        data = json.loads(payload)
        for key, value in data.items():
            fixed_value = fix_value(value)
            logging.debug(f"{measurement_name} -  Key: {key}, Value: {value}, Fixed value: {fixed_value}")
            insert_to_timescaledb(measurement_name, key, fixed_value)
        global processed_cnt
        processed_cnt += 1
    except Exception as e:
        logging.error(f"Error: {e}")
        global err_cnt
        err_cnt = err_cnt + 1

def init_database():
    cursor = timescale_connection.cursor()
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT 1 
            FROM pg_tables
            WHERE schemaname = 'public' AND tablename = 'loxone_measurements'
        );
    """)

    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS loxone_measurements (
            timestamp TIMESTAMPTZ NOT NULL,
            measurement_name TEXT NOT NULL,
            value_name TEXT NOT NULL,
            value DOUBLE PRECISION,
            value_str TEXT
        );
        SELECT create_hypertable('loxone_measurements', 'timestamp', if_not_exists => TRUE);
        create index if not exists idx_measurement_name_value_name on loxone_measurements(measurement_name, value_name);
        """)
        timescale_connection.commit()
        cursor.close()


def print_progress():
    global processed_cnt, err_cnt
    logging.info(f"Processed {processed_cnt} messages, errors: {err_cnt}")        


# read environment variables
mqtt_address = get_env_var('MQTT_ADDRESS')
mqtt_port = int(get_env_var('MQTT_PORT'))
loxone_mqtt_topic_name = get_env_var('LOXONE_MQTT_TOPIC_NAME')
timescaledb_host = get_env_var('POSTGRES_HOST')
timescaledb_port = get_env_var('POSTGRES_PORT')
timescaledb_user = get_env_var('POSTGRES_USER')
timescaledb_password = get_env_var('POSTGRES_PASSWORD')
timescaledb_dbname = get_env_var('POSTGRES_DBNAME')
verbose = str2bool(os.environ.get('LOXONE2TIMESCALE_VERBOSE'))

if verbose:
    logging.basicConfig(level=logging.DEBUG)
else:    
    logging.basicConfig(level=logging.INFO)

# connect to timescaledb
try:
    logging.info("Connecting to timescaledb")
    timescale_connection = psycopg2.connect(
        host=timescaledb_host,
        port=timescaledb_port,
        user=timescaledb_user,
        password=timescaledb_password,
        dbname=timescaledb_dbname
    )
    init_database()
except Exception as e:
    logging.error(f"Failed to connect to timescaledb: {e}")
    sys.exit(1)

# connect to mqtt
try:
    logging.info("Connecting to mqtt\n")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message
    if(verbose):
        client.enable_logger()
    client.connect(mqtt_address, mqtt_port)
    logging.info("Starting MQTT loop")
    client.loop_start()

    logging.info("Entering main loop")

    # Keep the script running
    while True:
        print_progress()
        sleep(5)
        pass
except Exception as e:
    logging.error(f"Failed to connect to mqtt: {e}")
    sys.exit(1)

