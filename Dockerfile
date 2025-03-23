# ---------------------------
# Stage 1: Download SnowSQL
# ---------------------------
FROM alpine:latest as downloader
WORKDIR /tmp

# Install curl
RUN apk add --no-cache curl

# Download SnowSQL
RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.24-linux_x86_64.bash

# ---------------------------
# Stage 2: Final Airflow image with Snowflake support
# ---------------------------
FROM apache/airflow:2.6.2

USER root

# Install needed packages
RUN apt-get update && \
    apt-get install -y curl unzip bash && \
    apt-get clean

# Create necessary directories and set permissions
RUN mkdir -p /home/airflow/.snowsql && \
    mkdir -p /home/airflow/.dbt && \
    chown -R airflow:root /home/airflow/.snowsql && \
    chown -R airflow:root /home/airflow/.dbt

# Copy the SnowSQL installer
COPY --from=downloader /tmp/snowsql-1.2.24-linux_x86_64.bash /tmp/

# Switch to airflow user for pip installations
USER airflow

# Install Snowflake Connector for Python and dbt
RUN pip install --no-cache-dir --user 'snowflake-connector-python[pandas]>=3.0.0' && \
    pip install --no-cache-dir --user dbt-snowflake

# Create Snowflake config directory
RUN echo "[connections]" > ~/.snowsql/config && \
    echo "accountname = \${SNOWFLAKE_ACCOUNT}" >> ~/.snowsql/config && \
    echo "username = \${SNOWFLAKE_USER}" >> ~/.snowsql/config && \
    echo "password = \${SNOWFLAKE_PASSWORD}" >> ~/.snowsql/config && \
    echo "dbname = \${SNOWFLAKE_DATABASE}" >> ~/.snowsql/config && \
    echo "schemaname = \${SNOWFLAKE_SCHEMA}" >> ~/.snowsql/config && \
    echo "warehousename = \${SNOWFLAKE_WAREHOUSE}" >> ~/.snowsql/config && \
    echo "rolename = \${SNOWFLAKE_ROLE}" >> ~/.snowsql/config && \
    chmod 600 ~/.snowsql/config

# Copy Python dependencies and install them
COPY --chown=50000:50000 requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Copy the custom entrypoint script and set executable permissions
COPY --chown=50000:50000 scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Test Snowflake connection using Python
RUN python -c "import snowflake.connector; print('Snowflake Connector version:', snowflake.connector.__version__)"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash", "-c", "airflow db init && \
    airflow users create --username $AIRFLOW_USERNAME --firstname Admin --lastname User --role Admin --email admin@example.com --password $AIRFLOW_PASSWORD && \
    airflow scheduler & airflow webserver"]