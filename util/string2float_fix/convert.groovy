import com.opencsv.CSVIterator
import com.opencsv.CSVReader
@Grab("org.influxdb:influxdb-java:2.20")
@Grab(group = 'org.slf4j', module = 'slf4j-api', version = '1.6.1')
@Grab(group = 'ch.qos.logback', module = 'logback-classic', version = '0.9.28')
@Grab('com.opencsv:opencsv:4.0')
import groovy.util.logging.Slf4j
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point
import org.influxdb.dto.Query

import java.util.concurrent.TimeUnit

@Slf4j
class Main {

    final SRC_INFLUX_ADDRESS = "srcinfluxdb"
    final SRC_INFLUX_PORT = 8086

    //final SRC_INFLUX_ADDRESS = "localhost"
    //final SRC_INFLUX_PORT = 8087


    final DST_INFLUX_ADDRESS = "influxdb"
    //final DST_INFLUX_ADDRESS = "localhost"
    final DST_INFLUX_PORT = 8086

    def IGNORED_COLUMN_NAMES = ['name', 'time']
    InfluxDB srcInfluxDB
    InfluxDB dstInfluxDB

    def start() {

        log.info("Convertor started")
        srcInfluxDB = InfluxDBFactory.connect("http://${SRC_INFLUX_ADDRESS}:${SRC_INFLUX_PORT}", "grafana", "grafana")
        srcInfluxDB.setDatabase("loxone")
        log.info("Connected to source influx")

        dstInfluxDB = InfluxDBFactory.connect("http://${DST_INFLUX_ADDRESS}:${DST_INFLUX_PORT}", "grafana", "grafana")
        dstInfluxDB.createDatabase("loxone")
        dstInfluxDB.setDatabase("loxone")
        dstInfluxDB.enableBatch(2000, 1000, TimeUnit.MILLISECONDS)
        log.info("Connected to destination influx")

        def measurements = getMeasurementNames(srcInfluxDB)
        
        log.info("Migrating data")
        def remainingMeasurements = measurements.size()
        measurements.each { measurement ->
            log.info("Processing measurement ${measurement}, remaining ${remainingMeasurements}")
            processMeasurement(measurement)
            remainingMeasurements--
        }

        log.info("Migration done")

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
        exec("""echo "select * from ${measurementName}" | influx -host ${SRC_INFLUX_ADDRESS} -port ${SRC_INFLUX_PORT} -database loxone -format csv >/tmp/dump.csv""").text

        log.info("Writing fixed data to influx")
        CSVIterator iterator = new CSVIterator(new CSVReader(new FileReader("/tmp/dump.csv")));
        def header = null
        for(String[] nextLine : iterator) {
            if(header == null) {
                header = nextLine
            } else {
                def fieldMap = extractCsvLine(header, nextLine)
                writeValues(fieldMap)
            }
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

    def exec(cmd) {
        def command=["/bin/bash", "-c", cmd]
        log.debug("Executing ${command}")
        return command.execute()
    }

    def extractCsvLine(String[] header, String[] fields) {
        def ret = [:]
        for (i in 0..<fields.size()) {
            ret[header[i]] = fields[i]
        }
        return ret
    }

    def writeValues(valueMap) {
        def measurementName = valueMap.remove("name")
        def time = valueMap.remove("time") as Long
        def pointBuilder = Point.measurement(measurementName).time(time, TimeUnit.NANOSECONDS)
        def fieldCount = 0
        valueMap.keySet().each { key ->
            def originalValue = valueMap[key]
            def fixedValue = (key in IGNORED_COLUMN_NAMES) ? originalValue : fixupValue(originalValue)
            if (fixedValue != null) {
                if(fixedValue instanceof String) { log.warn("${measurementName}.${key} is string (${fixedValue}), original value is (${originalValue})"); }
                pointBuilder = pointBuilder.addField(key, fixedValue)
                fieldCount++
            }
        }
        if (fieldCount > 0) {
            Point point = pointBuilder.build();
            dstInfluxDB.write(point)
        } else {
            log.warn("Zero fields for $valueMap for $measurementName")
        }
    }

}


def m = new Main()
m.start()


