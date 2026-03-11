# ─────────────────────────────────────────────────────────────────────────────
# FlinkSQL Studio — Dockerfile
#
# Build:  docker build -t your-registry/flinksql-studio:latest .
#
# Run:    docker run -p 3030:80 \
#           -e FLINK_GATEWAY_HOST=your-gateway-host \
#           -e FLINK_GATEWAY_PORT=8083 \
#           -e JOBMANAGER_HOST=your-jobmanager-host \
#           -e JOBMANAGER_PORT=8081 \
#           --network your-docker-network \
#           your-registry/flinksql-studio:latest
#
# Environment variables:
#   FLINK_GATEWAY_HOST  — hostname of the Flink SQL Gateway  (default: localhost)
#   FLINK_GATEWAY_PORT  — port of the Flink SQL Gateway      (default: 8083)
#   JOBMANAGER_HOST     — hostname of the Flink JobManager   (default: localhost)
#   JOBMANAGER_PORT     — port of the Flink JobManager REST  (default: 8081)
#
# The container must be on the same Docker network as your Flink services
# so it can resolve their hostnames.
# ─────────────────────────────────────────────────────────────────────────────

FROM nginx:1.25-alpine

LABEL org.opencontainers.image.title="FlinkSQL Studio"
LABEL org.opencontainers.image.description="Browser-based SQL IDE for Apache Flink"
LABEL org.opencontainers.image.source="https://github.com/coded-streams/flinksql-studio"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# envsubst: substitutes env vars into the nginx config template at startup
# wget: used by the healthcheck probe
RUN apk add --no-cache gettext wget

# ── Static assets ─────────────────────────────────────────────────────────────
# Copy the full studio/ directory so nginx can serve all modules:
#   index.html  — application shell
#   css/        — stylesheets
#   js/         — JavaScript modules
COPY studio/ /usr/share/nginx/html/

# ── nginx config ──────────────────────────────────────────────────────────────
# The .template extension tells the nginx docker-entrypoint to run envsubst
# on this file before starting nginx, replacing ${VAR} placeholders.
COPY nginx/studio.conf /etc/nginx/templates/default.conf.template
RUN  rm -f /etc/nginx/conf.d/default.conf

# ── Default environment variables ────────────────────────────────────────────
# Override these at runtime via -e flags or your docker-compose.yml.
ENV FLINK_GATEWAY_HOST=localhost \
    FLINK_GATEWAY_PORT=8083 \
    JOBMANAGER_HOST=localhost \
    JOBMANAGER_PORT=8081

# ── Custom entrypoint ─────────────────────────────────────────────────────────
# Runs envsubst on the nginx template then starts nginx.
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN  chmod +x /docker-entrypoint.sh

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD        ["nginx", "-g", "daemon off;"]