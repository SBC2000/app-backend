FROM node:12.7.0 AS build

WORKDIR /usr/src/app

# install dependencies
COPY package*.json ./
RUN npm install

# copy app
COPY . ./

# test
RUN npm run lint && npm run test

# build
RUN npm run build

FROM node:12.7.0-alpine

# install tini
RUN apk add --no-cache tini

WORKDIR /usr/src/app

# install dependencies
COPY package*.json ./
RUN npm ci --only=production

# copy transpiled app
COPY --from=build /usr/src/app/dist/*.js ./

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["tini"]
CMD ["node", "index.js"]