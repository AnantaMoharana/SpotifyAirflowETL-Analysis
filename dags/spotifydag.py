#import libraries
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from src.extract import authenitcate_api, get_songs_from_playlist, get_artist_info


#Create default argument for dag
default_args = {
    'owner': 'Ananta Moharana',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes= .5)
}

#Create dag instance
dag = DAG(
    'Spotify_Data_ETL',
    default_args = default_args,
    description = 'Gets data regarding the Top 50 Songs in the World and their artists',
    schedule = '@monthly'
    )

#Create start etl task
start_etl = DummyOperator(
    task_id = 'StartETL',  
    dag = dag
)

#Create the authentication operator
authenitcate=PythonOperator(
    task_id='AuthenticateAPI',
    python_callable=authenitcate_api,
    dag=dag

)

#Create the operataro to get the songs from the playlist
get_songs=PythonOperator(
    task_id='GetSongsFromPlaylist',
    python_callable=get_songs_from_playlist,
    dag=dag

)

#Create the operatar to get the artists information from the playlist
artist_info=PythonOperator(
    task_id='GetArtistInfo',
    python_callable=get_artist_info,
    dag=dag

)




start_etl >> authenitcate >> get_songs >> artist_info
