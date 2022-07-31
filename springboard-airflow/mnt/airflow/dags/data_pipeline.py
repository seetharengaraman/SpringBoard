"""
Simple Airflow Data Pipeline to check Apple and Tesla stock spreads
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime, timedelta

import yfinance as yf
import pandas as pd
import os

default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 7, 30),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }

global airflow_home 
airflow_home = os.environ['AIRFLOW_HOME']

def download_stock_data(stock_name):
    file_path = airflow_home + '/'+ stock_name + '_data.csv'
    if date.today().weekday() not in (5,6):
        start_date = date.today() 
    else:
        start_date = date.today() - timedelta(days=2)
        end_date = start_date + timedelta(days=1)
    df = yf.download(stock_name, start=start_date, end=end_date, interval='1m')
    df.to_csv(file_path, header=False)


def get_last_stock_spread(**kwargs):
    apple_file_name = airflow_home + '/data/'+ kwargs['ds'] +'/AAPL_data.csv' 
    tesla_file_name = airflow_home + '/data/'+ kwargs['ds'] +'/TSLA_data.csv'
    apple_data = pd.read_csv(apple_file_name,
         names=['date time','open','high','low','close','adj close','volume']).sort_values(by = "date time", ascending = False)
    tesla_data = pd.read_csv(tesla_file_name,
         names=['date time','open','high','low','close','adj close','volume']).sort_values(by = "date time", ascending = False)
    spread = [apple_data['high'][0] - apple_data['low'][0], tesla_data['high'][0] - tesla_data['low'][0]]
    return spread


dag = DAG(dag_id="marketvol",
         schedule_interval="0 6 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='source Apple and Tesla data' )

t0 = BashOperator(
        task_id="create_directory",
        bash_command="mkdir -p $AIRFLOW_HOME/data/{{ ds }}", #naming the folder with the current day
        dag = dag
    )

t1 = PythonOperator(
        task_id ="fetch_apple_stocks",
        python_callable = download_stock_data,
        op_kwargs={'stock_name':'AAPL'},
        dag = dag
    )

t2 = PythonOperator(
        task_id ="fetch_tesla_stocks",
        python_callable = download_stock_data,
        op_kwargs={'stock_name':'TSLA'},
        dag = dag
    )
templated_command="""
  mv ${AIRFLOW_HOME}/{{ params.stock_name }}_data.csv ${AIRFLOW_HOME}/data/{{ ds }}
"""

t3 = BashOperator(
        task_id="move_apple_to_data_location",
        bash_command=templated_command, #naming the folder with the current day
        params={'stock_name':'AAPL'},
        dag = dag
    )

t4 = BashOperator(
        task_id="move_tesla_to_data_location",
        bash_command=templated_command, #naming the folder with the current day
        params={'stock_name':'TSLA'},
        dag = dag
    )

t5 = PythonOperator(
        task_id ="query_stocks",
        python_callable = get_last_stock_spread,
        dag = dag
    )

t0 >> t1 >> t3
t0 >> t2 >> t4
[t3,t4] >> t5
