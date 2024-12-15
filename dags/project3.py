from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from modules.etl_top_countries import *
from modules.etl_total_film import *

from datetime import datetime

def fun_extract_top_countries():
    extract_transform_top_countries()

def fun_load_top_countries():
    load_top_countries()
    
def fun_extract_total_film():
    extract_total_film()

def fun_load_total_film():
    load_total_film()

with DAG(
    dag_id='project3',
    start_date=datetime(2024, 12, 14),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:
    
    start_task=EmptyOperator(
        task_id='start'
    )
    
    op_extract_transform_top_countries=PythonOperator(
        task_id='extract_transform_top_countries',
        python_callable=fun_extract_top_countries
    )
    
    op_load_top_countries=PythonOperator(
        task_id='load_top_countries',
        python_callable=fun_load_top_countries
    )
    
    op_extract_transform_total_film=PythonOperator(
        task_id='extract_transform_total_film',
        python_callable=fun_extract_total_film
    )
    
    op_load_total_film=PythonOperator(
        task_id='load_total_film',
        python_callable=fun_load_total_film
    )
    
    end_task=EmptyOperator(
        task_id='end'
    )
    
    start_task >> [op_extract_transform_top_countries, op_extract_transform_total_film]
    op_extract_transform_top_countries >> op_load_top_countries >> end_task
    op_extract_transform_total_film >> op_load_total_film >> end_task

