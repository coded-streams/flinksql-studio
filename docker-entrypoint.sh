#!/bin/sh
# docker-entrypoint.sh
# Works in Docker Compose, Kubernetes, and local environments.
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# ── Detect DNS resolver from /etc/resolv.conf ─────────────────────────────
# This works in all environments:
#   Docker Compose : 127.0.0.11  (Docker's embedded DNS)
#   Kubernetes     : cluster DNS IP from the pod's resolv.conf (e.g. 10.96.0.10)
#   Local / direct : host DNS   (e.g. 127.0.0.53, 8.8.8.8)
#
# If resolv.conf has no nameserver line (edge case), fall back to 8.8.8.8.
RESOLVER=$(awk 'NF==2 && $1=="nameserver" {print $2; exit}' /etc/resolv.conf)
RESOLVER=${RESOLVER:-8.8.8.8}
echo "  DNS resolver: ${RESOLVER}"

# ── Create UDF JAR storage directory (nginx WebDAV target) ────────────────
mkdir -p /var/www/udf-jars
chmod 755 /var/www/udf-jars

# ── Substitute env vars into nginx config ─────────────────────────────────
# Listing variables explicitly prevents envsubst from replacing nginx
# runtime variables like $host, $uri, $flink_gateway, etc.
envsubst '${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT} ${RESOLVER}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "nginx config written. Starting nginx..."
exec "$@"