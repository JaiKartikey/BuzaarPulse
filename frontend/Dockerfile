# syntax=docker/dockerfile:1.7

# --- Build the static bundle ------------------------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/ca; fi; \
    npm ci --no-audit --no-fund
COPY . .
RUN npm run build

# --- Serve it. The unprivileged nginx image runs as a non-root user on port 8080. ---------
FROM nginxinc/nginx-unprivileged:1.27-alpine AS runtime
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
# Where the API lives is decided at deploy time, not build time: the same image runs in compose and in Kubernetes.
ENV API_UPSTREAM=api:8000
EXPOSE 8080
