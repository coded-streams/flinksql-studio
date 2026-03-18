#!/bin/sh
# docker-entrypoint.sh
# Works in Docker Compose, Kubernetes, and local environments.
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# ── Detect DNS resolver from /etc/resolv.conf ─────────────────────────────
# Read the nameserver line from resolv.conf — this is the correct DNS for
# whichever environment we are in (Docker, Kubernetes, local).
# MUST be exported so envsubst can substitute ${RESOLVER} in the nginx template.
RESOLVER=$(awk 'NF==2 && $1=="nameserver" {print $2; exit}' /etc/resolv.conf)
RESOLVER=${RESOLVER:-127.0.0.11}
export RESOLVER
echo "  DNS resolver: ${RESOLVER}"

# ── Create UDF JAR storage directory ─────────────────────────────────────
mkdir -p /var/www/udf-jars
chmod 755 /var/www/udf-jars

# ── Write nginx config ────────────────────────────────────────────────────
# Listing all variables explicitly prevents envsubst from accidentally
# replacing nginx runtime variables like $host, $uri, $remote_addr, etc.
envsubst '${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT} ${RESOLVER}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "nginx config written. Starting nginx..."
exec "$@"