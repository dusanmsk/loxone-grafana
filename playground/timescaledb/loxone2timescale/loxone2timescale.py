import time
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

# TODO aj valuename dat do slovnika

flush_interval=10           # 10
progress_interval=60        # 60
reconnect_interval=30       # 30

err_cnt = 0
processed_cnt = 0
timescale_connection = None
mqtt_client = None
timescale_cache = []
last_flush_time = datetime.datetime.now()

def get_env_var(name):
    value = os.environ.get(name)
    assert value, f"{name} environment variable is not set."
    return value

def getLogLevel():
    level=str(os.getenv('LOXONE2TIMESCALE_LOGLEVEL', 'info')).lower()
    if level == 'debug':
        return logging.DEBUG
    elif level == 'info':
        return logging.INFO
    elif level == 'warning':
        return logging.WARNING
    elif level == 'error':
        return logging.ERROR
    elif level == 'critical':
        return logging.CRITICAL
    else:
        return logging.INFO

# set logging level
logging.basicConfig(level=getLogLevel(), format='%(asctime)s - %(levelname)s - %(message)s')


# read environment variables
mqtt_address = get_env_var('MQTT_ADDRESS')
mqtt_port = int(get_env_var('MQTT_PORT'))
loxone_mqtt_topic_name = get_env_var('LOXONE_MQTT_TOPIC_NAME')
timescaledb_host = get_env_var('POSTGRES_HOST')
timescaledb_port = get_env_var('POSTGRES_PORT')
timescaledb_user = get_env_var('POSTGRES_USER')
timescaledb_password = get_env_var('POSTGRES_PASSWORD')
timescaledb_dbname = get_env_var('POSTGRES_DBNAME')


# debug
#timescaledb_host = "localhost"
#mqtt_address = "localhost"

def connectToTimescale():
    global timescale_connection
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
        logging.info("Connected to timescaledb")
    except Exception as e:
        timescale_connection = None
        logging.error(f"Failed to connect to timescaledb: {e}")
        raise e


# name : id
dict_cache = {}
def get_or_create_dict(cursor, name):
    global dict_cache
    if not name in dict_cache:
        cursor.execute(f"""
            INSERT INTO _dictionary (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id;
        """, (name,))
        dict_cache[name] = cursor.fetchone()[0]
    return dict_cache[name]

def insert_measurement(cursor, timestamp, measurement_name, value_name, value):
    measurement_name_id=get_or_create_dict(cursor, measurement_name)
    value_name_id=get_or_create_dict(cursor, value_name)
    if isinstance(value, (int, float)):
        cursor.execute("""
            INSERT INTO _measurements (timestamp, measurement_name_id, value_name_id, value, value_str)
            VALUES (%s, %s, %s, %s, NULL)
        """, (timestamp, measurement_name_id, value_name_id, value))
    else:
        cursor.execute("""
            INSERT INTO _measurements (timestamp, measurement_name_id, value_name_id, value, value_str)
            VALUES (%s, %s, %s, NULL, %s)
        """, (timestamp, measurement_name_id, value_name_id, value))

flush_lock = threading.Lock()
def flush_cache():
    global timescale_cache, timescale_connection, err_cnt
    if timescale_cache:
        with flush_lock:
            try:
                logging.debug("flushing {} records".format(len(timescale_cache)))
                cursor = timescale_connection.cursor()
                for timestamp, measurement_name, value_name, value in timescale_cache:
                    insert_measurement(cursor, timestamp, measurement_name, value_name, value)
                timescale_connection.commit()
                cursor.close()
                timescale_cache.clear()
            except Exception as e:
                logging.error(f"Failed to insert data to timescaledb: {e}.")
                err_cnt += 1

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

def mqtt_on_disconnect(a, b, c, rc, e):
    logging.error(f"Disconnected from MQTT broker with code {rc}")


def init_database():
    cursor = timescale_connection.cursor()
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT 1 
            FROM pg_tables
            WHERE schemaname = 'public' AND tablename = '_measurements'
        );
    """)

    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute("""
     CREATE TABLE IF NOT EXISTS _dictionary (
    id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS _measurements (
                                                    timestamp TIMESTAMPTZ NOT NULL,
                                                    measurement_name_id SMALLINT NOT NULL REFERENCES _dictionary(id),
                                                    value_name_id SMALLINT NOT NULL REFERENCES _dictionary(id),
                                                    value DOUBLE PRECISION,
                                                    value_str TEXT
);

SELECT create_hypertable('_measurements', 'timestamp', if_not_exists => TRUE);
ALTER TABLE _measurements
    ADD CONSTRAINT unique_measurement UNIQUE (timestamp, measurement_name_id, value_name_id);

-- TODO indexes

create view measurements as
select timestamp, mn.name as measurement_name, vn.name as value_name, value, value_str from _measurements inner join _dictionary mn on _measurements.measurement_name_id = mn.id
inner join _dictionary vn on _measurements.value_name_id = vn.id;
;

-- TODO
--ALTER TABLE _measurements SET (timescaledb.compress);
--SELECT add_compression_policy('_measurements', compress_after => INTERVAL '60d');
        """)
        timescale_connection.commit()
        cursor.close()


def print_progress():
    global processed_cnt, err_cnt
    logging.info(f"Processed {processed_cnt} messages, errors: {err_cnt}, cache size: {len(timescale_cache)}")        


def reconnect_handler():
    global timescale_connection
    while True:
        try:
            if timescale_connection is None or timescale_connection.closed:
                connectToTimescale()
            sleep(reconnect_interval)
        except Exception as e:
            logging.error(f"Failed to reconnect to timescaledb: {e}")
            sleep(reconnect_interval)
            


def main():
    logging.info("Starting loxone2timescale")
    # start a separate thread for reconnecting to timescaledb
    reconnect_thread = threading.Thread(target=reconnect_handler)
    reconnect_thread.start()    

    # connect to mqtt
    try:
        logging.info("Connecting to mqtt\n")
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
        mqtt_client.on_connect = mqtt_on_connect
        mqtt_client.on_message = mqtt_on_message
        mqtt_client.on_disconnect = mqtt_on_disconnect
        if(getLogLevel() <= logging.INFO):
            mqtt_client.enable_logger()
        mqtt_client.connect(mqtt_address, mqtt_port)
        logging.info("Starting MQTT loop")
        mqtt_client.loop_start()

        logging.info("Entering main loop")

        # Keep the script running
        while True:
            print_progress()
            sleep(progress_interval)
            pass
    except Exception as e:
        logging.error(f"Failed to connect to mqtt: {e}")
        sys.exit(1)


main()
