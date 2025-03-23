#!/bin/bash
set -e

mkdir -p /home/airflow/.snowsql

cat <<EOF > /home/airflow/.snowsql/config
[connections.my_connection]
accountname = ${SNOWFLAKE_ACCOUNT}
username = ${SNOWFLAKE_USER}
password = ${SNOWFLAKE_PASSWORD}
rolename = ${SNOWFLAKE_ROLE}
warehousename = ${SNOWFLAKE_WAREHOUSE}
dbname = ${SNOWFLAKE_DATABASE}
schemaname = ${SNOWFLAKE_SCHEMA}
EOF

chown -R airflow: /home/airflow/.snowsql

exec "$@"
