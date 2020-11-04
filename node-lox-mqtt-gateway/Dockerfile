FROM node:12-alpine

#RUN apt-get update && apt-get install -f wget bash git         # debian
RUN apk update && apk add wget bash git                         # alpine

RUN npm config set user 0; npm config set unsafe-perm true

RUN cd /tmp && git clone https://github.com/dusanmsk/node-lox-mqtt-gateway.git nodelox
RUN cd /tmp/nodelox && npm -g install

COPY config.json /default.json
COPY run.sh /run.sh
RUN  chmod a=rx /run.sh

CMD [ "/bin/sh", "/run.sh" ]
