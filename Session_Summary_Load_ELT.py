from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline will run after Session_Stage_Load_ETL is exected. Below two tables will get created and loaded before executing this DAG
 - user_session_channel
 - session_timestamp
"""

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            
            # Added duplicate check using row_number
            dups_check = f"select {primary_key}, row_number() over (partition by {primary_key} order by {primary_key}) as rn \
                from DEV.ANALYTICS.SESSION_SUMMARY \
                    qualify row_number() over (partition by {primary_key} order by {primary_key}) > 1 limit 1;"
            print(dups_check)
            cur.execute(dups_check)
            result_2 = cur.fetchone()
            print(result_2, result_2[1])

            if int(result[1]) > 1 or int(result_2[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: Duplicate Check 1 {result}, Duplicate check 2 {result_2}")
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise


with DAG(
    dag_id = 'BuildELT_CTAS',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    table = "dev.analytics.session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(table, select_sql, primary_key='sessionId')