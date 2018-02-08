FROM alpine


RUN apk update && apk add nodejs nodejs-npm
RUN npm config set user 0; npm config set unsafe-perm true
RUN npm install -g node-lox-mqtt-gateway

COPY config.json /default.json
COPY run.sh /run.sh

CMD [ "/bin/sh", "/run.sh" ]




