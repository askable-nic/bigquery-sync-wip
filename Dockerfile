FROM node:22-alpine

WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json tsconfig.json .env ./
RUN npm install

# Copy source code and build
COPY src/ ./src/
RUN npm run build

# Copy built files
COPY dist/ ./

# Start the application
CMD [ "node", "./index.js" ]