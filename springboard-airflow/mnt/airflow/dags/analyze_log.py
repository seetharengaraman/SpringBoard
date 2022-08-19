"""
Simple Airflow Data Pipeline to analyze log files created by marketvol dag for AAPL and TSLA stocks 
spread
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from pathlib import Path

import os
import pendulum

local_tz = pendulum.timezone("America/Detroit")

default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 8, 15,tzinfo=local_tz),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }

global airflow_home 
global error_list
global file_list
airflow_home = os.environ['AIRFLOW_HOME']
error_list =[]
file_list = Path(airflow_home).rglob('*.log')

def fetch_apple_files(**context):
    apple_files = []
    for file in file_list:
        if 'apple' in str(file):
            apple_files.append(str(file))
    context['ti'].xcom_push(key='apple_files',value=apple_files)

def fetch_non_apple_files(**context):
    non_apple_files = []
    for file in file_list:
        if 'apple' not in str(file):
            non_apple_files.append(str(file))
    context['ti'].xcom_push(key='non_apple_files',value=non_apple_files)

def analyze_file(key_value,**context):
    if key_value =='apple':
        file_list = context['ti'].xcom_pull(key='apple_files')
    else:
        file_list = context['ti'].xcom_pull(key='non_apple_files')
    count = 0
    error_list =[]
    for file in file_list:
        with open(file) as f:
            for line in f.readlines():
                if 'ERROR' in line:
                    count += 1
                    error_list.append(line)
    
    context['ti'].xcom_push(key=key_value+'_count',value=count)
    context['ti'].xcom_push(key=key_value+'_error_list',value=error_list)

def print_error_log(**context):
    total_count = context['ti'].xcom_pull(key='apple_count',task_ids='analyze_apple_files') + context['ti'].xcom_pull(key='non_apple_count',task_ids='analyze_non_apple_files')
    error_list.extend(context['ti'].xcom_pull(key='apple_error_list',task_ids='analyze_apple_files'))
    error_list.extend(context['ti'].xcom_pull(key='non_apple_error_list',task_ids='analyze_non_apple_files'))
    print(f"Total number of errors: {total_count}")
    print("Here are all the errors:")
    for i in error_list:
        print(i)

dag = DAG(dag_id="analyzelogs",
         schedule_interval="0 18 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='Analyze logs')

t01 = PythonOperator(
        task_id = "fetch_apple_files",
        python_callable = fetch_apple_files,
        provide_context=True,
        dag = dag
        )

t02 = PythonOperator(
        task_id = "fetch_non_apple_files",
        python_callable = fetch_non_apple_files,
        provide_context=True,
        dag = dag
        )

t1 = PythonOperator(
        task_id = "analyze_apple_files",
        python_callable = analyze_file,
        op_kwargs={'key_value':'apple'},
        provide_context=True,
        dag = dag
        ) 

t2 = PythonOperator(
        task_id = "analyze_non_apple_files",
        python_callable = analyze_file,
        op_kwargs={'key_value':'non_apple'},
        provide_context=True,
        dag = dag
        ) 

t3 = PythonOperator(
        task_id = "print_error_log",
        python_callable = print_error_log,
        provide_context=True,
        dag = dag
        )

t01 >> t1
t02 >> t2
[t1,t2] >> t3