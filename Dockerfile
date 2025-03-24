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
    
    # Install needed packages and fix SSL
    RUN apt-get update && \
        apt-get install -y curl unzip bash openjdk-11-jdk python3-pip ca-certificates && \
        apt-get clean && \
        update-ca-certificates && \
        rm -rf /var/lib/apt/lists/*
    
    # Create necessary directories and set permissions
    RUN mkdir -p /home/airflow/.snowsql && \
        mkdir -p /home/airflow/.dbt && \
        chown -R airflow:root /home/airflow/.snowsql && \
        chown -R airflow:root /home/airflow/.dbt
    
    # Copy the SnowSQL installer
    COPY --from=downloader /tmp/snowsql-1.2.24-linux_x86_64.bash /tmp/


    ARG SNOWFLAKE_ACCOUNT=""
    ARG SNOWFLAKE_USER=""
    ARG SNOWFLAKE_PASSWORD=""
    ARG SNOWFLAKE_DATABASE=""
    ARG SNOWFLAKE_SCHEMA=""
    ARG SNOWFLAKE_WAREHOUSE=""
    ARG SNOWFLAKE_ROLE=""
    
    # Switch to airflow user for pip installations
    USER airflow
    
    # Upgrade pip first and install certificates
    RUN pip install --no-cache-dir --upgrade pip certifi
    
    # Install Snowflake Connector for Python and dbt
    RUN pip install --no-cache-dir 'snowflake-connector-python[pandas]>=3.0.0' && \
        pip install --no-cache-dir dbt-snowflake
    
    # Create Snowflake config directory
    RUN echo "[connections]" > ~/.snowsql/config && \
        echo "accountname = ${SNOWFLAKE_ACCOUNT}" >> ~/.snowsql/config && \
        echo "username = ${SNOWFLAKE_USER}" >> ~/.snowsql/config && \
        echo "password = ${SNOWFLAKE_PASSWORD}" >> ~/.snowsql/config && \
        echo "dbname = ${SNOWFLAKE_DATABASE}" >> ~/.snowsql/config && \
        echo "schemaname = ${SNOWFLAKE_SCHEMA}" >> ~/.snowsql/config && \
        echo "warehousename = ${SNOWFLAKE_WAREHOUSE}" >> ~/.snowsql/config && \
        echo "rolename = ${SNOWFLAKE_ROLE}" >> ~/.snowsql/config && \
        chmod 600 ~/.snowsql/config
    
    # Install pyspark separately first
    RUN pip install --no-cache-dir 'pyspark==3.4.1'
    
    # Copy Python dependencies and install them
    COPY --chown=50000:50000 requirements.txt /tmp/requirements.txt
    RUN pip install --no-cache-dir -r /tmp/requirements.txt
    
    # Copy the custom entrypoint script and set executable permissions
    COPY --chown=50000:50000 scripts/entrypoint.sh /entrypoint.sh
    RUN chmod +x /entrypoint.sh
    
    # Test Snowflake connection using Python
    RUN python -c "import snowflake.connector; print('Snowflake Connector version:', snowflake.connector.__version__)"
    
    # Use an ENTRYPOINT and CMD that initialize the database safely
    ENTRYPOINT ["/entrypoint.sh"]
    CMD ["bash", "-c", \
        "airflow db init || true && \
         airflow users create --username ${AIRFLOW_USERNAME} --firstname Admin --lastname User --role Admin --email admin@example.com --password ${AIRFLOW_PASSWORD} || true && \
         airflow scheduler & airflow webserver"]