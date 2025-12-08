#!/bin/bash
set -e

# ClickHouse connection settings
HOST="${CLICKHOUSE_HOST:-clickhouse}"
USER="${CLICKHOUSE_USER:-default}"
PASSWORD="${CLICKHOUSE_PASSWORD}"

# Build clickhouse-client command with credentials
if [ -n "$PASSWORD" ]; then
    CLICKHOUSE_CMD="clickhouse-client --host $HOST --user $USER --password $PASSWORD"
else
    CLICKHOUSE_CMD="clickhouse-client --host $HOST --user $USER"
fi

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
until $CLICKHOUSE_CMD --query "SELECT 1" > /dev/null 2>&1; do
    echo "ClickHouse is unavailable - sleeping"
    sleep 2
done

echo "ClickHouse is up - executing SQL scripts"

# Execute all SQL files in /sql directory in order
for sql_file in /sql/*.sql; do
    if [ -f "$sql_file" ]; then
        echo "Executing: $sql_file"
        $CLICKHOUSE_CMD --multiquery < "$sql_file"
        echo "✓ Completed: $sql_file"
    fi
done

echo "All SQL scripts executed successfully!"
