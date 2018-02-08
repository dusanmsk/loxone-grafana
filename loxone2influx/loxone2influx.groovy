@Grab(group = 'org.eclipse.paho', module = 'mqtt-client', version = '0.4.0')
@Grab("org.influxdb:influxdb-java:2.7")
@Grab(group = 'org.slf4j', module = 'slf4j-api', version = '1.6.1')
@Grab(group = 'ch.qos.logback', module = 'logback-classic', version = '0.9.28')

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import org.eclipse.paho.client.mqttv3.*
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point

import java.util.concurrent.TimeUnit

@Slf4j
class Main {

    // TODO config file
    def FIRE_EVEN_NOT_CHANGED_SEC = 1800        // each 30 min refire stalled values
    def RECONNECT_TIME_SEC = 30

    def mqttUrl = "tcp://mosquitto:1883"

    def jsonSlurper = new JsonSlurper()
    InfluxDB influxDB
    MqttClient client
    String dbName = "loxone";

    def previousValues = [:]
    def fireTimestamps = [:]


    def start() throws Exception {
        refireThread.start()
        def connected = false
        while (true) {
            try {
                if (!connected) {
                    log.info("Connecting ...")

                    influxDB = InfluxDBFactory.connect("http://influxdb:8086", "grafana", "grafana")
                    influxDB.createDatabase(dbName)
                    influxDB.setDatabase(dbName)
                    log.info("Connected to influx")

                    client = new MqttClient(mqttUrl, "mqtt2influx", null)
                    client.connect()

                    client.setCallback(new MqttCallback() {
                        @Override
                        void connectionLost(Throwable throwable) {
                            log.warn "MQTT connection lost. Will reconnect in ${RECONNECT_TIME_SEC} seconds."
                            connected = false
                        }

                        @Override
                        void messageArrived(String topic, MqttMessage mqttMessage) {
                            try {
                                processMessage(topic, new String(mqttMessage.payload))
                            } catch (Exception e) {
                                e.printStackTrace()
                            }
                        }

                        @Override
                        void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {}
                    })

                    client.subscribe("lox/#")
                    log.info("Connected to mqtt")
                    log.info("Ready")
                    connected = true
                }
            } catch (Exception e) {
                log.error("Exception, disconnecting. Will reconnect in ${RECONNECT_TIME_SEC} seconds", e)
                if (influxDB) {
                    influxDB.close()
                }
                if (client) {
                    client.close()
                }
                connected = false
            }

            Thread.sleep(RECONNECT_TIME_SEC * 1000);
        }

    }

    def getStatName(topic) {
        def s = topic.toString().replace("lox/", "").replace("/state", "").replaceAll("[^A-Za-z0-9_]", "_");
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
            log.info("Skipping ${topic} ${message} ${value_name}")
        }
    }

    def fireMessage(topic, message) {
        def now = System.currentTimeMillis()
        def value_name = getStatName(topic)
        Point.Builder point = Point.measurement(value_name).time(now, TimeUnit.MILLISECONDS)
        def data = jsonSlurper.parseText(message)
        data.each { i ->
            point = point.addField(i.key, fixupValue(i.value))
        }
        influxDB.write(point.build())
        fireTimestamps[topic] = now
        previousValues[topic] = message
        log.info("Storing ${topic} ${message} ${value_name}")
    }

    def refireStoredMessages() {
        def now = System.currentTimeMillis()
        previousValues.keySet().each { topic ->
            def message = previousValues[topic]
            def lastFired = fireTimestamps[topic]
            lastFired = lastFired == null ? 0 : lastFired
            if (now - lastFired > FIRE_EVEN_NOT_CHANGED_SEC * 1000) {
                fireMessage(topic, message)
            }
        }
    }

    String fixupValue(value) {
        switch (value) {
            case 'on': return "1"
            case 'off': return "0"
        }
        return value.toString().replaceAll("[^0-9.-]", "");
    }


    Thread refireThread = new Thread(new Runnable() {
        @Override
        void run() {
            while (true) {
                try {
                    refireStoredMessages()
                } finally {
                    Thread.sleep(FIRE_EVEN_NOT_CHANGED_SEC * 1000)
                }
            }
        }
    })
}

def m = new Main()
m.start()
