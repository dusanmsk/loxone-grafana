FROM alpine:3.17

RUN apk update && apk add mosquitto
COPY ./mosquitto.conf /etc/mosquitto/mosquitto.conf

EXPOSE 1883 9001

CMD [ "/usr/sbin/mosquitto",  "-c",  "/etc/mosquitto/mosquitto.conf" ]
