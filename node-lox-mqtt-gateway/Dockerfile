FROM alpine

RUN apk update && apk add nodejs nodejs-npm wget bash
RUN npm config set user 0; npm config set unsafe-perm true
#RUN npm install -g --unsafe-perm node-lox-mqtt-gateway
RUN cd /tmp && wget https://github.com/dusanmsk/node-lox-mqtt-gateway/archive/master.zip && unzip master.zip && cd node-lox-mqtt-gateway-master &&  npm -g install

COPY config.json /default.json
COPY run.sh /run.sh

CMD [ "/bin/sh", "/run.sh" ]
