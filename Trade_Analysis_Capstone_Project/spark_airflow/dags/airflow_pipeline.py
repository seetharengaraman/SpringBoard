"""
Airflow Data Pipeline to perform Trade Analysis
"""

from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import os
import pendulum
from datetime import date, datetime, timedelta

local_tz = pendulum.timezone("America/Detroit")

default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 11, 2,tzinfo=local_tz),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }

global airflow_home 
airflow_home = os.environ['AIRFLOW_HOME']

dag = DAG(dag_id="TradeAnalysis",
         schedule_interval="0 18 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='Ingest and Analyze Stock Market Data' )

t0 = BashOperator(
        task_id="determine_current_directory",
        bash_command="pwd", 
        dag = dag
    )

t1 = BashOperator(
        task_id="ingest_stock_data",
        bash_command="python $AIRFLOW_HOME/dags/ingest_trade_data.py", 
        dag = dag
    )

t2 = BashOperator(
        task_id="analyze_trade_and_quote_data",
        bash_command="python $AIRFLOW_HOME/dags/trade_analysis.py", 
        dag = dag
    )



t0 >> t1 >> t2
