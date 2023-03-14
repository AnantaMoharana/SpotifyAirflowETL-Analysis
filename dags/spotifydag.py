#import libraries
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from src.extract import authenitcate_api, get_songs_from_playlist, get_artist_info, get_song_audio_quality
from src.transform import transform_data
from src.load import load_tables, json_to_csv

#Get todays date
today_str = datetime.today().strftime('%Y-%m-%d')
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
authenticate=PythonOperator(
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

#Create the operatar to get the audio features of each song
audio_quality=PythonOperator(
    task_id='GetAudioQuality',
    python_callable=get_song_audio_quality,
    dag=dag

)

#Create the operatar to get the audio features of each song
data_transformations=PythonOperator(
    task_id='TransformData',
    python_callable=transform_data,
    dag=dag

)

#Put the song fact in S3
stage_song_fact=PythonOperator(
    task_id='SongFactToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/song_fact.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_fact') }}"
    },
    dag=dag
)

#Put the song dimension in S3
stage_song_dim=PythonOperator(
    task_id='SongDimToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/song_dim.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_dim') }}"
    },
    dag=dag
)
#Put the song artist brudge in S3
stage_song_arist_bridge=PythonOperator(
    task_id='SongArtistBridgeToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/song_arist_bridge.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_arist_bridge') }}"
    },
    dag=dag
)
#Put the song artist fact in S3
stage_artist_fact=PythonOperator(
    task_id='ArtistFactToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/artist_fact.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_fact') }}"
    },
    dag=dag
)
#Put the Artist Dimension in S3
stage_artist_dime=PythonOperator(
    task_id='ArtistDimesnionToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/artist_dimension.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_dimension') }}"
    },
    dag=dag
)

#Put the artist genre bridge in S3
stage_artist_genre_bridge=PythonOperator(
    task_id='ArtistGenreBridgeToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/artist_genre_bridge.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_genre_bridge') }}"
    },
    dag=dag
)

#Put the genre bridge in S3
stage_genre=PythonOperator(
    task_id='GenreToS3',
    python_callable=load_tables,
    op_kwargs={
        'key':today_str+'/genre_bridge.json',
        'bucketname':'spotify-top-50',
        "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='genre') }}"
    },
    dag=dag
)

#Convert the jsons to CSV
json_csv=PythonOperator(
    task_id='JsonToCSV',
    python_callable=json_to_csv,
    op_kwargs={
        'pay_load':{"s3_bucket":"spotify-top-50", "s3_folder":today_str}
    },
    dag=dag
)


#Create end etl task
finished = DummyOperator(
    task_id = 'ETLDone',  
    dag = dag
)


start_etl >> \
authenticate >> \
get_songs >> \
[artist_info, audio_quality] >> \
data_transformations >> \
[stage_song_fact, stage_song_dim, stage_song_arist_bridge, stage_artist_fact, stage_artist_dime, stage_artist_genre_bridge, stage_genre] >> \
json_csv >> \
finished