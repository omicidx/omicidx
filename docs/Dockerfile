# syntax=docker/dockerfile:1

FROM node:22-alpine AS builder
WORKDIR /app

# Cache layer: install deps before copying source so source changes
# don't bust the npm install cache.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# ---

FROM caddy:2-alpine AS runtime

COPY --from=builder /app/dist /srv
COPY Caddyfile /etc/caddy/Caddyfile

EXPOSE 80
