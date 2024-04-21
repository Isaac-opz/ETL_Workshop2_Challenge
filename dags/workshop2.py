from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from code.transformations.Spotify_Transformations import read_csv, transform_csv
from code.transformations.Grammy_Transformations import read_db, transform_db
from code.transformations.Merge import merge_datasets

default_args = {
    'owner': 'izkopz',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['isaac.piedrahita@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('etl_workflow2',
          default_args=default_args,
          description='ETL DAG for Spotify and Grammy data',
          schedule_interval=timedelta(days=1),
          catchup=False)

read_spotify_data = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

transform_spotify_data = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_csv,
    dag=dag,
)

read_grammy_data = PythonOperator(
    task_id='read_db',
    python_callable=read_db,
    dag=dag,
)

transform_grammy_data = PythonOperator(
    task_id='transform_db',
    python_callable=transform_db,
    dag=dag,
)

merge_data = PythonOperator(
    task_id='merge_datasets',
    python_callable=merge_datasets,
    dag=dag,
)

read_spotify_data >> transform_spotify_data >> merge_data
read_grammy_data >> transform_grammy_data >> merge_data
merge_data >> load_data_to_db