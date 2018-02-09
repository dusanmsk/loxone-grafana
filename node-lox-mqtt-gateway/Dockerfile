FROM alpine


RUN apk update && apk add nodejs nodejs-npm
RUN npm config set user 0; npm config set unsafe-perm true
#RUN npm install -g --unsafe-perm node-lox-mqtt-gateway

COPY config.json /default.json
COPY tmp_fixed_gateway /tmp_fixed_gateway
RUN  cd /tmp_fixed_gateway; npm -g install

COPY run.sh /run.sh

CMD [ "/bin/sh", "/run.sh" ]




