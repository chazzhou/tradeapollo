import pandas as pd
import requests
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from collections import defaultdict
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from contextlib import contextmanager

from DataCollection import price_data,price_spot_market,dayAheadPrices,prices,trade,trade_Net,trade_netflows,mibgas_dayahead,fetch_and_update_volumes,get_price,get_capacity,get_flow,get_intraday,get_consumption


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}




# Define default_args for the DAG
default_args = {
    'owner': 'Hachem Sfar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'save_to_postgresql_dag',
    default_args=default_args,
    description='DAG for saving data to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Define the functions to be executed
functions = [price_data,price_spot_market,dayAheadPrices,prices,trade,trade_Net,trade_netflows,mibgas_dayahead,fetch_and_update_volumes,get_price,get_capacity,get_flow,get_intraday,get_consumption]

# Define a dummy task to trigger all functions in parallel
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define a dummy task for successful completion
success_task = DummyOperator(
    task_id='success',
    dag=dag,
)

# Define a dummy task for failure
failure_task = DummyOperator(
    task_id='failure',
    dag=dag,
)

# Define email notification task
email_task = EmailOperator(
    task_id='send_email',
    to='hachem.sfar@supcom.tn',
    subject='Airflow DAG Execution Status',
    html_content='<p>All tasks have been successfully executed.</p>',
    dag=dag,
)

# Set up dependencies and parallel execution
start_task >> success_task
start_task >> failure_task

for i, function in enumerate(functions):
    task_id = f'task_{i}'
    execute_function = PythonOperator(
        task_id=task_id,
        python_callable=function,
        dag=dag,
    )
    execute_function >> success_task
    execute_function >> failure_task

# Set up email notification after all tasks are complete
[success_task, failure_task] >> email_task
