FROM node:22-alpine

WORKDIR /app
COPY ./package.json ./package-lock.json ./tsconfig.json .env .
COPY ./src ./src
RUN npm install
RUN npm run build
COPY ./dist ./

# Start the application
CMD [ "node", "./index.js" ]
# CMD ["find", ".", "-maxdepth", "2", "-type", "d"]