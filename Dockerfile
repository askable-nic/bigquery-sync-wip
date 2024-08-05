FROM node:22-alpine

WORKDIR /app

COPY package*.json tsconfig.json .env* ./
RUN npm install

COPY src/ ./src/

RUN npm run build

USER node
HEALTHCHECK NONE
CMD [ "node", "./dist/index.js" ]