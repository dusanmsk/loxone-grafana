import traceback

from lib import *
import logging
import threading
import os, sys
from time import sleep
import paho.mqtt.client as mqtt
import json
from questdb.ingress import Sender, TimestampNanos

auto_flush_rows = 2       # todo 50
auto_flush_interval = 5000
progress_interval=60        # 60

err_cnt = 0
processed_cnt = 0
mqtt_client = None

# set logging level
logging.basicConfig(level=getLogLevel('LOXONE2QUESTDB_LOGLEVEL'), format='%(asctime)s - %(levelname)s - %(message)s')

# read environment variables
mqtt_host = get_env_var('MQTT_HOST')
mqtt_port = int(get_env_var('MQTT_PORT'))
loxone_mqtt_topic_name = get_env_var('LOXONE_MQTT_TOPIC_NAME')

questdb_host = get_env_var('QUESTDB_HOST')
questdb_port = get_env_var('QUESTDB_PORT')
questdb_username = get_env_var('QUESTDB_USERNAME')
questdb_password = get_env_var('QUESTDB_PASSWORD')

questdb_util = QuestDbUtil(questdb_host, questdb_port, questdb_username, questdb_password, auto_flush_rows, auto_flush_interval)
questdb_util.connect()

def mqtt_on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        topic = f"{loxone_mqtt_topic_name}/#"
        logging.info(f"Subscribing to {topic}")
        client.subscribe(topic)
    else:
        logging.error("Failed to connect to MQTT broker")
        sys.exit(1)


def convertTypes(fields):
    columns = {}
    for key, value in fields.items():
        value = fix_value(value)
        if (isinstance(value, str)):
            key = f"{key}_str"
        columns[key] = value
    return columns


def mqtt_on_message(client, userdata, msg):
    try:
        global questdb_util, processed_cnt
        payload = msg.payload.decode()
        measurement_name = mqtt_topic_to_measurent_name(msg.topic, loxone_mqtt_topic_name)
        data = json.loads(payload)
        columns = convertTypes(data)
        questdb_util.insert_to_questdb(measurement_name, columns, TimestampNanos.now())
        processed_cnt += 1
    except Exception as e:
        logging.error(f"Error: {e}")
        traceback.print_exc()
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
    if (getLogLevel('LOXONE2QUESTDB_LOGLEVEL') <= logging.INFO):
        mqtt_client.enable_logger()
    mqtt_client.connect(mqtt_host, mqtt_port)
    logging.info("Starting MQTT loop")
    mqtt_client.loop_forever(retry_first_connection=True)

def reporting_handler():
    while True:
        print_progress()
        sleep(progress_interval)

def main():
    logging.info("Starting loxone2questdb")
    reporting_thread = threading.Thread(target=reporting_handler)
    reporting_thread.start()
    connectToMQTTAndLoopForever()

main()
