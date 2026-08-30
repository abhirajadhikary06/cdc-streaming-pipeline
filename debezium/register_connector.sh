#!/bin/bash
set -e

# Load environment variables from root .env file if present
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "Registering Debezium PostgreSQL CDC Connector with Kafka Connect..."

# Construct JSON payload dynamically with environment variables
payload=$(cat <<EOF
{
  "name": "neon-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "${NEON_HOST}",
    "database.port": "${NEON_PORT:-5432}",
    "database.user": "${NEON_USER}",
    "database.password": "${NEON_PASSWORD}",
    "database.dbname": "${NEON_DATABASE}",
    "database.sslmode": "require",
    "topic.prefix": "neon_cdc",
    "table.include.list": "public.customer_transactions",
    "publication.name": "dbz_publication",
    "slot.name": "neon_cdc_slot",
    "publication.autocreate.mode": "filtered",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
EOF
)

# POST payload to Kafka Connect REST API
curl -s -X POST -H "Content-Type: application/json" \
  --data "$payload" \
  http://localhost:8083/connectors | jq .

echo -e "\nConnector registration request completed."