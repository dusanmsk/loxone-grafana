@Grab("org.influxdb:influxdb-java:2.20")
@Grab(group = 'org.slf4j', module = 'slf4j-api', version = '1.6.1')
@Grab(group = 'ch.qos.logback', module = 'logback-classic', version = '0.9.28')
@Grab('com.opencsv:opencsv:4.0')
import groovy.util.logging.Slf4j
import org.eclipse.paho.client.mqttv3.*
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult
import org.influxdb.impl.TimeUtil

import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@Slf4j
class Main {

    final SRC_INFLUX_PORT = 8086
    final DST_INFLUX_PORT = 8086

    def IGNORED_COLUMN_NAMES = ['name', 'time']
    InfluxDB srcInfluxDB
    InfluxDB dstInfluxDB

    def start() {

        log.info("Convertor started")
        srcInfluxDB = InfluxDBFactory.connect("http://srcinfluxdb:${SRC_INFLUX_PORT}", "grafana", "grafana")
        srcInfluxDB.setDatabase("loxone")
        log.info("Connected to source influx")

        dstInfluxDB = InfluxDBFactory.connect("http://influxdb:${DST_INFLUX_PORT}", "grafana", "grafana")
        dstInfluxDB.createDatabase("loxone")
        dstInfluxDB.setDatabase("loxone")
        dstInfluxDB.enableBatch(2000, 1000, TimeUnit.MILLISECONDS)
        log.info("Connected to destination influx")

        def measurements = getMeasurementNames(srcInfluxDB)
        
        log.info("Migrating data")
        measurements.each { measurement ->
            processMeasurement(measurement)
        }

    }


    def getMeasurementNames(influxDb) {
        def measurementNames = []
        def measurementsQueryResult = influxDb.query(new Query("show measurements", "loxone"))
        measurementsQueryResult.results*.series*.values.each { i ->
            i.each { names ->
                names.each { name ->
                    measurementNames << name[0]
                }
            }
        }
        return measurementNames
    }

    def processMeasurement(measurementName) {
        log.info("Processing measurement: $measurementName")
        srcInfluxDB.query(new Query("select * from ${measurementName}", "loxone"), 1000, new Consumer<QueryResult>() {
            @Override
            void accept(QueryResult queryResult) {
                queryResult.results.each { result ->
                    result.series.each { serie ->
                        serie.values.each { valueList ->
                            def valueMap = convertValuesToMap(serie.columns, valueList)
                            def time = TimeUtil.fromInfluxDBTimeFormat(valueMap.remove("time"))
                            def pointBuilder = Point.measurement(measurementName).time(time, TimeUnit.MILLISECONDS)
                            def fieldCount = 0
                            valueMap.keySet().each { key ->
                                def value = (key in IGNORED_COLUMN_NAMES) ? value : fixupValue(valueMap[key])
                                if (value != null) {
                                    if(value instanceof String) { log.warn("${measurementName}.${key} is string (${value}), original value is (${valueMap[key]})"); }
                                    pointBuilder = pointBuilder.addField(key, value)
                                    fieldCount++
                                }
                            }
                            pointProcessed()
                            if (fieldCount > 0) {
                                Point point = pointBuilder.build();
                                dstInfluxDB.write(point)
                            } else {
                                log.warn("Zero fields for $valueList for $measurementName")
                            }
                        }
                    }
                }
            }
        })
    }

    def processedPoints = 0
    def lastPrintTimestamp = System.currentTimeMillis()

    def pointProcessed() {
        processedPoints++
        if (System.currentTimeMillis() - lastPrintTimestamp > 10000) {
            log.info "Processed $processedPoints points"
            lastPrintTimestamp = System.currentTimeMillis()
        }
    }

    def convertValuesToMap(List fieldNames, List values) {
        def aMap = [:]
        for (int i = 0; i < fieldNames.size(); i++) {
            def value = values[i]
            if (value != null && !value.toString().isBlank()) {
                aMap[fieldNames[i]] = value
            }
        }
        return aMap
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
                    if(ch == '-' && i == 0) {
                        numericValue << ch
                    }
                    else if(ch.isDigit()) {
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

}


def m = new Main()
m.start()

