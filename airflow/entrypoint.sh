#!/bin/bash
set -e

echo "Running Airflow DB migrate..."
airflow db migrate

echo "Creating postgres_default connection..."
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host db \
  --conn-port 5432 \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-schema "$POSTGRES_DB" || true

echo "Starting Airflow standalone..."
exec airflow standalone
