from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import logging

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_file_format(stage_name, file_format):
    logging.info(f"Creating file format for stage: {stage_name}")
    cur = return_snowflake_conn()
    url = Variable.get("Session_Details_URL")
    create_stage = f"CREATE STAGE IF NOT EXISTS {stage_name} URL='{url}' FILE_FORMAT=({file_format});"
    logging.info(create_stage)
    cur.execute(create_stage)

#Stage tables user_session_channel and session_timestamp are full load, create table and load table are placed in single transaction
@task
def load_stage_table(stg_table, stage_file, file_format, column_list):
    logging.info(f"Loading data into {stg_table} from {stage_file}")
    cur = return_snowflake_conn()
    load_sql = f"COPY INTO {stg_table} FROM @{stage_file};"
    logging.info(load_sql)
    create_sql = f"CREATE OR REPLACE TABLE {stg_table} {column_list}"
    logging.info(create_sql)
    try:
        cur.execute("BEGIN;")
        cur.execute(create_sql)
        logging.info(create_sql)
        cur.execute(load_sql)
        logging.info(load_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f'Failed to execute SQL: {load_sql}. Error: {str(e)}')
        raise

with DAG(
    dag_id='Session-Stage-Load',
    start_date=datetime(2024, 10, 22),
    catchup=False,
    tags=['ETL', 'STAGE'],
    schedule='30 20 * * *'
) as dag:
    
    target_table_session_channel = "dev.raw_data.user_session_channel"
    target_table_session_timestamp = "dev.raw_data.session_timestamp"
    session_channel_columns = "( userId INT NOT NULL, sessionId VARCHAR(32) PRIMARY KEY, channel VARCHAR(32) DEFAULT 'direct' );"
    session_timestamp_columns = "( sessionId varchar(32) primary key, ts timestamp );"
    file_format_params = 'TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY=\'"\''
    stage_name = "dev.raw_data.blob_stage"
    session_channel_file = "dev.raw_data.blob_stage/user_session_channel.csv"
    session_timestamp_file = "dev.raw_data.blob_stage/session_timestamp.csv"
    
    CTAS_trigger = TriggerDagRunOperator(
        task_id="CTAS_trigger",
        trigger_dag_id="BuildELT_CTAS",
        execution_date='{{ ds }}',
        reset_dag_run=True
    )


    create_file_format(stage_name, file_format_params) >> \
    load_stage_table(target_table_session_channel, session_channel_file, stage_name, session_channel_columns) >> \
    CTAS_trigger

    create_file_format(stage_name, file_format_params) >> \
    load_stage_table(target_table_session_timestamp, session_timestamp_file, stage_name, session_timestamp_columns) >> \
    CTAS_trigger
