FROM node:22-alpine

RUN addgroup app && adduser -S -G app app
RUN mkdir /app && chown -R app:app /app
USER app
WORKDIR /app

# Copy package files and install dependencies
COPY --chown=app:app package*.json tsconfig.json .env ./
RUN npm install

# Copy source code and build
COPY --chown=app:app src/ ./src/
RUN npm run build

# Start the application
CMD [ "node", "./dist/index.js" ]