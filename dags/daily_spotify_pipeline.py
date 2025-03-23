from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import snowflake.connector
import os

def execute_snowflake_query(sql_file):
    """Execute a SQL file against Snowflake"""
    # Read SQL from file
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    # Execute SQL
    cur = conn.cursor()
    for statement in sql.split(';'):
        if statement.strip():
            cur.execute(statement)
    
    # Close connection
    cur.close()
    conn.close()

default_args = {
    'owner': 'spotify_data_pipeline',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_spotify_pipeline',
    default_args=default_args,
    description='Run Spotify ingestion -> Bronze -> Gold transformations daily',
    schedule_interval=None,  # midnight daily
    catchup=False
) as dag:

    # 1) Produce to Kafka
    produce_spotify_tracks = BashOperator(
        task_id='produce_spotify_tracks',
        bash_command='python /opt/airflow/src/producers/spotify_eu_tracks_producer.py'
    )

    produce_spotify_artists = BashOperator(
        task_id='produce_spotify_artists',
        bash_command='python /opt/airflow/src/producers/spotify_eu_artists_producer.py'
    )

    # 2) Consume to S3
    consume_to_s3 = BashOperator(
        task_id='consume_to_s3',
        bash_command='python /opt/airflow/src/consumers/kafka_to_s3.py'
    )

    # 3) Snowflake Bronze scripts
    create_wh = PythonOperator(
        task_id='create_wh',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/01_create_wh.sql']
    )

    create_db_schema = PythonOperator(
        task_id='create_db_schema',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/02_create_db_schema.sql']
    )

    create_storage_integration = PythonOperator(
        task_id='create_storage_integration',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/03_create_storage_integration.sql']
    )

    create_stage = PythonOperator(
        task_id='create_stage',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/04_create_stage.sql']
    )

    create_bronze_tables = PythonOperator(
        task_id='create_bronze_tables',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/05_create_bronze_tables.sql']
    )

    load_bronze_tables = PythonOperator(
        task_id='load_bronze_tables',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/06_load_bronze_tables.sql']
    )

    curated_bronze_tables = PythonOperator(
        task_id='curated_bronze_tables',
        python_callable=execute_snowflake_query,
        op_args=['/opt/airflow/sql/bronze/07_curate_bronze_tables.sql']
    )

    # 4) dbt transformations for Gold layer
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt/my_spotify_project && dbt run'
    )

    # Task dependencies
    produce_spotify_tracks >> produce_spotify_artists >> consume_to_s3
    consume_to_s3 >> create_wh >> create_db_schema >> create_storage_integration >> \
        create_stage >> create_bronze_tables >> load_bronze_tables >> curated_bronze_tables >> run_dbt