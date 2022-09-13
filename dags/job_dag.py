from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

from etl import etl_jobs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['c.mendozar@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'scrapping_etl',
    default_args = default_args,
    description = 'A jobs finder web scrapping for get on board website using mongodb to save data',
    schedule_interval = timedelta(days = 1),
    start_date = days_ago(2),
    tags = ['scrapping'],

) as dag:

    run_etl = PythonOperator(task_id = "etl" , 
                provide_context=True,
                python_callable = etl_jobs,
                op_kwargs = {'day': '{{ds}}'}
                )
    
run_etl 
