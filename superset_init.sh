#!/bin/bash
set -e

echo "Initializing Superset..."

# Initialize database if not already done
if [ ! -f /app/superset_home/.superset_initialized ]; then
  echo "First run - initializing Superset database..."

  # Upgrade database
  superset db upgrade

  # Create admin user
  superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@basekick.net \
    --password admin

  # Initialize Superset
  superset init

  # Mark as initialized
  touch /app/superset_home/.superset_initialized

  echo "===================="
  echo "Superset initialized!"
  echo "===================="
  echo "Login: admin / admin"
  echo "CHANGE PASSWORD IMMEDIATELY!"
  echo "===================="
else
  echo "Superset already initialized"
  superset db upgrade
fi

# Start Superset
echo "Starting Superset on port 8088..."
gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 4 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
