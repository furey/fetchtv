FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache tzdata \
    && cp /usr/share/zoneinfo/Australia/Sydney /etc/localtime \
    && echo "Australia/Sydney" > /etc/timezone

COPY package*.json ./

RUN npm ci --production --no-fund --no-audit

COPY . .

ENTRYPOINT ["node", "fetchtv.js"]
