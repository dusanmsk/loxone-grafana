import os
import re
from datetime import datetime

import requests
from questdb.ingress import Sender, TimestampNanos
import logging

def fixColumns(columns, field_types):
    ret = {}
    for key in columns:
        value = columns[key]
        # skip null values
        if value == None:
            continue
        # do explicit column data conversion
        field_type = field_types[key] if field_types else None
        if field_type == "float" or field_type == "integer":
            value = fix_value(value)
            if type(value) == str:
                key = f"{key}_str"
        if field_type == "string":
            value = str(value)

        ret[key] = value
    return ret


# prevedie hodnotu na cislo alebo string. U hodnot kde je cislo a string (napriklad "1.0 kW") sa pokusi extrahovat 1.0 ako cislo
# vrati povodnu hodnotu ak sa konverzia nepodarila
def fix_value(value):
    if isinstance(value, (int, float)):
        return float(value)
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
            return value

def mqtt_topic_to_measurent_name(topic, loxone_mqtt_topic_name):
    s = topic.replace(f"{loxone_mqtt_topic_name}/", "").replace("/state", "")
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    while "__" in s:
        s = s.replace("__", "_")
    if s.endswith("_"):
        s = s[:-1]
    return s

def get_env_var(name):
    value = os.environ.get(name)
    assert value, f"{name} environment variable is not set."
    return value

def getLogLevel(env_variable_name):
    level=str(os.getenv(env_variable_name, 'info')).lower()
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


class QuestDbUtil:

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sender.close()

    def __init__(self, questdb_host, questdb_port, questdb_username, questdb_password, auto_flush_rows, auto_flush_interval):
        self.conf = f'http::addr={questdb_host}:{questdb_port};username={questdb_username};password={questdb_password};auto_flush_rows={auto_flush_rows};auto_flush_interval={auto_flush_interval};'
        self.questdb_host = questdb_host
        self.questdb_port = questdb_port
        self.sender = Sender.from_conf(self.conf)

    def connect(self):
        logging.debug("Connecting to questdb")
        self.sender.establish()
        logging.info("Connected to questdb")

    def insert_to_questdb(self, measurement_name, columns, timestampNs):
        logging.debug(f"Inserting to QuestDB: {measurement_name}, {columns}, {timestampNs}")
        measurement_name = measurement_name[:127]  # max 127 characters
        self.sender.row(
            measurement_name,
            columns=columns,
            at=timestampNs
        )

    def getQuestDbTableColumnTypes(self, table_name):
        column_types = {}
        url = f"http://{self.questdb_host}:{self.questdb_port}/exec"
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """
        response = requests.get(url, params={"query": query})
        if response.status_code == 200:
            data = response.json()
            for row in data["dataset"]:
                column_name, data_type = row
                column_types[column_name] = data_type
        return column_types


    def get_questdb_oldest_timestamp(self, measurement):
        try:
            query = f"SELECT min(timestamp) FROM {measurement}"
            url = f"http://{self.questdb_host}:{self.questdb_port}/exec"
            response = requests.get(url, params={"query": query})
            date = datetime.datetime.now()
            if response.status_code == 200:
                results = response.json()
                date_str = results['dataset'][0][0]
                date = self.parse_timestamp(date_str)
            return date
        except Exception as e:
            return datetime.now()

    def parse_timestamp(self, ts):
        date = None
        if '.' in ts:
            date = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            date = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return date.replace(tzinfo=datetime.timezone.utc)
