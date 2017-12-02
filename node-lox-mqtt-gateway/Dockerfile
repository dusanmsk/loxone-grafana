FROM alpine


RUN apk update && apk add nodejs nodejs-npm

RUN npm install -g node-lox-mqtt-gateway

COPY config.json /default.json

CMD ["lox-mqtt-gateway", "--NODE_CONFIG_DIR=/" ]



