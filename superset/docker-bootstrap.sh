#!/usr/bin/env bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname ShopFlow \
  --lastname Admin \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@shopflow.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init
python /app/pythonpath/register_trino.py

exec gunicorn \
  --bind "0.0.0.0:8088" \
  --access-logfile "-" \
  --error-logfile "-" \
  --workers 2 \
  --worker-class gthread \
  --threads 4 \
  --timeout 120 \
  "superset.app:create_app()"
