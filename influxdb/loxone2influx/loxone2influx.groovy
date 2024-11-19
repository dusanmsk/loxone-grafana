@Grab("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.4")
@Grab("org.influxdb:influxdb-java:2.7")

@Grab(group = 'org.slf4j', module = 'slf4j-api', version = '2.0.16')
@Grab(group = 'org.slf4j', module = 'slf4j-jdk14', version = '2.0.16')
@Grab('io.github.cdimascio:dotenv-java:3.0.0')
@Grab(group = 'org.slf4j', module = 'slf4j-api', version = '2.0.1')
@Grab(group = 'org.slf4j', module = 'slf4j-simple', version = '2.0.1')


import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import io.github.cdimascio.dotenv.Dotenv
import org.eclipse.paho.client.mqttv3.*
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point

import java.util.concurrent.TimeUnit

class Config {

    static LOCAL_DEBUG = false

    static FIRE_EVEN_NOT_CHANGED_SEC

    static MQTT_HOST

    static MQTT_PORT

    static LOXONE_MQTT_TOPIC_NAME

    static INFLUX_DB_NAME

    static INFLUXDB_HOST

    static INFLUXDB_PORT

    static INFLUXDB_USER

    static INFLUXDB_PASSWORD

    static LOXONE_TO_INFLUX_LOGLEVEL

    static readEnvironmentVariables() {
        Map<String, String> env =  System.getenv()
        if (Config.LOCAL_DEBUG) {
            Dotenv dotenv = Dotenv.configure()
                    .directory("/home/msk/work/github/loxone-grafana/")
                    .filename(".env.local")
                    .load()
            env = [:]
            dotenv.entries().each { e ->
                env.put(e.key, e.value)
            }
        }

        FIRE_EVEN_NOT_CHANGED_SEC = env.get("FIRE_EVEN_NOT_CHANGED_SEC").toInteger()
        MQTT_HOST = env.get("MQTT_HOST")
        MQTT_PORT = env.get("MQTT_PORT")
        LOXONE_MQTT_TOPIC_NAME = env.get("LOXONE_MQTT_TOPIC_NAME")
        INFLUX_DB_NAME = env.get("INFLUXDB_NAME");
        INFLUXDB_HOST = env.get("INFLUXDB_HOST");
        INFLUXDB_PORT = env.get("INFLUXDB_PORT");
        INFLUXDB_USER = env.get("INFLUXDB_USER");
        INFLUXDB_PASSWORD = env.get("INFLUXDB_PASSWORD");
        LOXONE_TO_INFLUX_LOGLEVEL = env.get("LOXONE_TO_INFLUX_LOGLEVEL");
    }

}

Config.readEnvironmentVariables()
System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", Config.LOXONE_TO_INFLUX_LOGLEVEL?.toLowerCase())

//import ch.qos.logback.classic.Level
//import ch.qos.logback.classic.Logger

@Slf4j
class Main {

    def jsonSlurper = new JsonSlurper()

    InfluxDB influxDB

    MqttClient client

    def previousValues = [:]

    def fireTimestamps = [:]

    def start() throws Exception {
        refireThread.setDaemon(true)
        refireThread.start()
        def influxdbAddressString = "http://${Config.INFLUXDB_HOST}:${Config.INFLUXDB_PORT}";
        log.info("Connecting to influx at ${influxdbAddressString}")
        log.info("FIRE_EVEN_NOT_CHANGED_SEC=${Config.FIRE_EVEN_NOT_CHANGED_SEC}")

        influxDB = InfluxDBFactory.connect(influxdbAddressString, Config.INFLUXDB_USER, Config.INFLUXDB_PASSWORD)
        influxDB.createDatabase(Config.INFLUX_DB_NAME)
        influxDB.setDatabase(Config.INFLUX_DB_NAME)
        influxDB.enableBatch(10, 2, TimeUnit.SECONDS)
        log.info("Connected to influx")

        client = new MqttClient("tcp://${Config.MQTT_HOST}:${Config.MQTT_PORT}", "mqtt2influx", null)
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions()
        client.connect(mqttConnectOptions)

        client.setCallback(new MqttCallbackExtended() {

            @Override
            void connectComplete(boolean b, String s) {
                subscribe()
            }

            @Override
            void connectionLost(Throwable throwable) {
                log.warn "MQTT connection lost. Exiting."
                System.exit(1)
            }

            @Override
            void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                try {
                    processMessage(topic, new String(mqttMessage.payload))
                } catch (Exception e) {
                    e.printStackTrace()
                    System.exit(2)
                }
            }

            @Override
            void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

            }
        })
        subscribe()
    }

    def subscribe() {
        log.info("Connected to mqtt")
        client.subscribe("${Config.LOXONE_MQTT_TOPIC_NAME}/#")
        log.info("Ready")
    }

    def getStatName(topic) {
        def s = topic.toString().replace("${Config.LOXONE_MQTT_TOPIC_NAME}/", "").replace("/state", "").replaceAll("[^A-Za-z0-9_]", "_");
        while (s.contains("__")) {
            s = s.replaceAll("__", "_")
        }
        if (s.endsWith("_")) {
            s = s[0..-2]
        }
        return s
    }

    def processMessage(topic, message) {
        def value_name = getStatName(topic)
        if (previousValues[topic] != message) {
            fireMessage(topic, message)
        } else {
            log.debug("Skipping ${topic} ${message} ${value_name}")
        }
    }

    def fireMessage(topic, message) {
        try {
            def now = System.currentTimeMillis()
            def value_name = getStatName(topic)
            Point.Builder point = Point.measurement(value_name).time(now, TimeUnit.MILLISECONDS)
            def data = jsonSlurper.parseText(message)
            def fieldCount = 0
            data.each { i ->
                def value = fixupValue(i.value)
                if (value != null) {
                    point = point.addField(i.key, value)
                    fieldCount++
                }
            }
            if (fieldCount > 0) {
                influxDB.write(point.build())
                fireTimestamps[topic] = now
                previousValues[topic] = message
                log.debug("Storing ${topic} ${message} ${value_name}")
            }
        } catch (Exception e) {
            log.error("Failed to store to influx", e)
            System.exit(3)
        }
    }

    def refireStoredMessages() {
        def now = System.currentTimeMillis()
        previousValues.keySet().each { topic ->
            def message = previousValues[topic]
            def lastFired = fireTimestamps[topic]
            lastFired = lastFired == null ? 0 : lastFired
            if (now - lastFired > Config.FIRE_EVEN_NOT_CHANGED_SEC * 1000) {
                fireMessage(topic, message)
            }
        }
    }


    def fixupValue(value) {
        try {
            value = value.toString()
            // convert known textual values to numeric
            value = value.replace("on", "1")
            value = value.replace("off", "0")

            // extract numeric part of value (value should follow any text, like '-65.0 F' or '1250.5 kWh' or fuckups like '-12.33 some.thing-nasty') and convert it to float
            if (value[0] == '-' || (value[0] as Character).isDigit() || value[0] == '.') {
                def numericValue = []
                def dotCnt = 0
                for (int i = 0; i < value.length(); i++) {
                    char ch = value[i]
                    if (ch == '-' && i == 0) {
                        numericValue << ch
                    } else if (ch.isDigit()) {
                        numericValue << ch
                    } else if (ch == '.' && dotCnt == 0) {
                        numericValue << ch
                        dotCnt++
                    } else {
                        break
                    }
                }
                return numericValue.join("") as Float
            }
        } catch (Exception e) {
            // on any error or value that is not possible to convert to float return null
        }
        return null
    }

    Thread refireThread = new Thread(new Runnable() {

        @Override
        void run() {
            while (true) {
                try {
                    refireStoredMessages()
                } finally {
                    Thread.sleep(Config.FIRE_EVEN_NOT_CHANGED_SEC * 1000)
                }
            }
        }
    })
}

def m = new Main()
m.start()
