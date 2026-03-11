# ─────────────────────────────────────────────────────────────────────────────
# FlinkSQL Studio — Dockerfile
# Builds a tiny nginx image that serves the IDE and proxies API calls.
#
# Build:  docker build -t codedstreams/flinksql-studio:latest .
# Run:    docker run -p 3030:80 \
#           -e FLINK_GATEWAY_HOST=my-cluster \
#           -e FLINK_GATEWAY_PORT=8083 \
#           codedstreams/flinksql-studio:latest
# ─────────────────────────────────────────────────────────────────────────────

FROM nginx:1.25-alpine

LABEL org.opencontainers.image.title="FlinkSQL Studio"
LABEL org.opencontainers.image.description="Browser-based SQL IDE for Apache Flink"
LABEL org.opencontainers.image.authors="Nestor A. A <nestorabiawuh@gmail.com>"
LABEL org.opencontainers.image.source="https://github.com/coded-streams/flinksql-studio"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Install envsubst (part of gettext) for runtime config substitution
RUN apk add --no-cache gettext wget

# Copy static assets
COPY studio/ /usr/share/nginx/html/
COPY nginx/studio.conf /etc/nginx/templates/default.conf.template

# Remove default nginx config — we supply our own via template
RUN rm -f /etc/nginx/conf.d/default.conf

# Runtime environment variables with defaults
ENV FLINK_GATEWAY_HOST=localhost \
    FLINK_GATEWAY_PORT=8083 \
    JOBMANAGER_HOST=localhost \
    JOBMANAGER_PORT=8081

# Entrypoint: substitute env vars into nginx config, then start nginx
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
