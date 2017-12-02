import groovy.json.JsonSlurper
@Grab(group = 'org.eclipse.paho', module = 'mqtt-client', version = '0.4.0')
@Grab("org.influxdb:influxdb-java:2.7")
import org.eclipse.paho.client.mqttv3.*
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point

import java.util.concurrent.TimeUnit

class Main {

    def mqttUrl = "tcp://mosquitto:1883"

    def jsonSlurper = new JsonSlurper()
    InfluxDB influxDB
    String dbName = "loxone";

    def previousValues = [:]


    def main() throws Exception {

        influxDB = InfluxDBFactory.connect("http://influxdb:8086", "grafana", "grafana")
        influxDB.createDatabase(dbName)
        influxDB.setDatabase(dbName)

        while (true) {

            try {
                MqttClient client = new MqttClient(mqttUrl, "mqtt2influx", null)
                client.connect()

                client.setCallback(new MqttCallback() {
                    @Override
                    void connectionLost(Throwable throwable) {}

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

                Thread.sleep(30000)
            } catch (Exception e) {
                e.printStackTrace()
                Thread.sleep(5000);
            }

        }
    }

    def getStatName(topic) {
        def s = topic.toString().replace("lox/", "").replace("/state", "").replaceAll("[^A-Za-z0-9_]", "_");
        while (s.contains("__")) {
            s = s.replaceAll("__", "_")
        }
        if(s.endsWith("_")) {
            s = s[0..-2]
        }
        return s
    }

    def processMessage(topic, message) {
        def value_name = getStatName(topic)
        print "${topic} ${message} ${value_name}"
        if(previousValues[topic] != message) {      // do not store duplicities
            Point.Builder point = Point.measurement(value_name).time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            def data = jsonSlurper.parseText(message)
            data.each { i ->
                point = point.addField(i.key, cleanupNotNumber(i.value))
            }
            influxDB.write(point.build())
            previousValues[topic] = message
            println " - stored"
        } else {
            println " - skipped"
        }
    }

    String cleanupNotNumber(value) {
        return value.toString().replaceAll("[^0-9.-]", "");
    }
}

def m = new Main()
m.main()