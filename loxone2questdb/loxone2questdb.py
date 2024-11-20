from datetime import timedelta
import logging
import threading, datetime
import os, sys
from time import sleep
import paho.mqtt.client as mqtt
import json
import re
from questdb.ingress import Sender, TimestampNanos

# TODO aj valuename dat do slovnika

auto_flush_rows = 200
auto_flush_interval = 5000
progress_interval=60        # 60

err_cnt = 0
processed_cnt = 0
mqtt_client = None

def get_env_var(name):
    value = os.environ.get(name)
    assert value, f"{name} environment variable is not set."
    return value

def getLogLevel():
    level=str(os.getenv('LOXONE2QUESTDB_LOGLEVEL', 'info')).lower()
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
mqtt_host = get_env_var('MQTT_HOST')
mqtt_port = int(get_env_var('MQTT_PORT'))
loxone_mqtt_topic_name = get_env_var('LOXONE_MQTT_TOPIC_NAME')

questdb_host = get_env_var('QUESTDB_HOST')
questdb_port = get_env_var('QUESTDB_PORT')
questdb_username = get_env_var('QUESTDB_USERNAME')
questdb_password = get_env_var('QUESTDB_PASSWORD')

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


def insert_to_questdb(measurement_name, columns, at):
    logging.debug(f"Inserting to QuestDB: {measurement_name}, {columns}, {at}")
    global questdb_sender
    measurement_name = measurement_name[:127]       # max 127 characters
    questdb_sender.row(
        measurement_name,
        # convert all numeric values to float
        columns={k: float(v) if isinstance(v, (int, float)) else v for k, v in columns.items()},
        at = at
    )

questdb_sender = None
def mqtt_on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        measurement_name = get_measurement_name(msg.topic, loxone_mqtt_topic_name)
        data = json.loads(payload)
        columns = {key:fix_value(value) for key, value in data.items()}
        insert_to_questdb(measurement_name, columns, TimestampNanos.now())
        global processed_cnt
        processed_cnt += 1
    except Exception as e:
        logging.error(f"Error: {e}")
        global err_cnt
        err_cnt = err_cnt + 1

def mqtt_on_disconnect(a, b, c, rc, e):
    logging.error(f"Disconnected from MQTT broker with code {rc}")

def print_progress():
    global processed_cnt, err_cnt
    logging.info(f"Processed {processed_cnt} messages, errors: {err_cnt}")

mqtt_client = None
def connectToMQTTAndLoopForever():
    global mqtt_client
    logging.info(f"Connecting to mqtt at {mqtt_host}:{mqtt_port}")
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message
    mqtt_client.on_disconnect = mqtt_on_disconnect
    if (getLogLevel() <= logging.INFO):
        mqtt_client.enable_logger()
    mqtt_client.connect(mqtt_host, mqtt_port)
    logging.info("Starting MQTT loop")
    mqtt_client.loop_forever(retry_first_connection=True)

def reporting_handler():
    # Keep the script running
    while True:
        print_progress()
        sleep(progress_interval)


def main():
    logging.info("Starting loxone2questdb")
    reporting_thread = threading.Thread(target=reporting_handler)
    reporting_thread.start()

    conf = f'http::addr={questdb_host}:{questdb_port};username={questdb_username};password={questdb_password};auto_flush_rows={auto_flush_rows};auto_flush_interval={auto_flush_interval};'
    with Sender.from_conf(conf) as sender:
        global questdb_sender
        questdb_sender = sender
        connectToMQTTAndLoopForever()

main()
