#import libraries
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from src.extract import authenitcate_api


#Create default argument for dag
default_args = {
    'owner': 'Ananta Moharana',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2)
}

#Create dag instance
dag = DAG(
    'Spotify_Data_ETL',
    default_args = default_args,
    description = 'Gets data regarding the Top 50 Songs in the World and their artists',
    schedule_interval = '@monthly'
    )

#Create start etl task
start_etl = DummyOperator(
    task_id = 'StartSETL',  
    dag = dag
)

#Create the get songs from a playlist tag
authenitcate=PythonOperator(
    task_id='AuthenticateAPI',
    python_callable=authenitcate_api,
    dag=dag

)


start_etl >> authenitcate