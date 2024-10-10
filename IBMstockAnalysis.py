
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract_get_data(symbol):
  vantage_api_key = Variable.get("vantage_api_key")
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
  r = requests.get(url)
  data = r.json()
  results = []
  for d in data["Time Series (Daily)"]:
    temp_dict = data["Time Series (Daily)"][d]
    temp_dict["date"] = d   # added the date to the dict
    temp_dict["symbol"] = symbol #Added sybols to dictionary
    results.append(temp_dict)

  return results

@task
def transform_90_days(records):
  date_90_days_ago = datetime.now() - timedelta(days=90)
  date_90_days_ago_str = date_90_days_ago.strftime("%Y-%m-%d")
  results_90_days = [r for r in records if r["date"] >= date_90_days_ago_str]
  return results_90_days

@task
def load_records(con, table, results):
  try:

    con.execute("BEGIN;")
    con.execute(f"""
  CREATE OR REPLACE TABLE {table} (
  price_date date,
  open float,
  high float,
  low float,
  close float,
  volume integer,
  symbol varchar,
  primary key (price_date, symbol)
  );""")
    count = 0
    for r in results:
      open = r["1. open"]
      high = r["2. high"]
      low = r["3. low"]
      close = r["4. close"]
      volume = r["5. volume"]
      date = r["date"]
      symbol = r["symbol"]
      insert_sql = f"INSERT INTO {table} (symbol, open, high, low, close, volume, price_date) VALUES ('{symbol}', {open}, {high}, {low}, {close}, {volume}, '{date}');"
      con.execute(insert_sql)
      count += 1
    con.execute("COMMIT;")
  except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'IBMStockAnalysis',
    start_date = datetime(2024,10,8),
    catchup=False,
    tags=['ETL'],
    schedule = '30 20 * * *'
) as dag:
    target_table = "dev.raw_data.ibm_stock_price"
    cur = return_snowflake_conn()
    symbol = "IBM"
        
    data = extract_get_data(symbol)
    lines = transform_90_days(data)
    load_records(cur, target_table, lines)
