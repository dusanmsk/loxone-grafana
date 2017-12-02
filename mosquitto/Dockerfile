FROM alpine

RUN apk update && apk add mosquitto

EXPOSE 1883 9001

CMD [ "/usr/sbin/mosquitto" ]
