#!/bin/sh

# prepare config file for node-lox-mqtt-gateway
cat<<EOF > /default.json
{
    "winston": [{
        "Console": {
            "level": "${NODE_LOX_MQTT_LOGLEVEL}",
            "colorize": true,
            "timestamp": true
        }
    }],
    "mqtt": {
        "host": "mqtt://${MQTT_HOST}:${MQTT_PORT}",
        "options": {
            "clientId": "node-lox-mqtt-gateway"
        }
    },
    "miniserver": {
        "mqtt_prefix": "${LOXONE_MQTT_TOPIC_NAME}",
        "host": "${LOXONE_ADDRESS}",
        "username": "${LOXONE_USERNAME}",
        "password": "${LOXONE_PASSWORD}",
        "readonly": true
    }
}
EOF

# cat /default.json; sleep 10

while true; do
    lox-mqtt-gateway --NODE_CONFIG_DIR=/
done

