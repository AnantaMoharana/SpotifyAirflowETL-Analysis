#import libraries
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from src.extract import authenitcate_api, get_songs_from_playlist, get_artist_info, get_song_audio_quality
from src.transform import transform_data, validate_data
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
with DAG(
    'Spotify_Data_ETL',
    default_args = default_args,
    description = 'Gets data regarding the Top 50 Songs in the World and their artists',
    schedule = '@monthly'
    ) as dag:

    #Create start etl task
    start_etl = EmptyOperator(
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
    #Validate and put Song Fact in S3
    with TaskGroup('ValidateAndLoadSongFact') as PrepareLoadSongFact:
        #Validate the song fact
        validate_song_fact=PythonOperator(
            task_id='ValidateSongFact',
            python_callable=validate_data,
            op_kwargs={
                'columns':['track_id','popularity','date'],
                "table": "song_fact",
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_fact') }}"
            },
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
        validate_song_fact >> stage_song_fact

    #Validate and put Song Dim in S3
    with TaskGroup('ValidateAndLoadSongDim') as PrepareLoadSongDim:
        #Validate the song dimenesion
        validate_song_dim=PythonOperator(
            task_id='ValidateSongDim',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_dim') }}",
                'columns':['song_name','track_id','is_explicit','acousticness', 'danceability','duration_ms','energy','instrumentalness','key','liveness','loudness','speechiness','tempo','valence','mode'],
                "table": "song_dim"
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

        validate_song_dim >> stage_song_dim

    #Validate and put Song Artist Bridge in S3
    with TaskGroup('ValidateAndLoadSongArtistBridge') as PrepareSongArtistBridge:
        #Validate the song dimenesion
        validate_song_arist_bridge=PythonOperator(
            task_id='ValidateSongArtistBridge',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}",
                'columns':['track_id','artist_id'],
                "table": "song_artist_bridge"
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
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}"
            },
            dag=dag
        )
        validate_song_arist_bridge >> stage_song_arist_bridge

    #Validate and put Artist Fact in S3
    with TaskGroup('ValidateAndLoadArtistFact') as PrepareArtistFact:
        #Validate the artist fact
        validate_artist_fact=PythonOperator(
            task_id='ValidateArtistFact',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_fact') }}",
                'columns':['artist_id','artist_name','followers','popularity','date'],
                "table": "artist_fact"
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

        validate_artist_fact >> stage_artist_fact

    #Validate and put Artist Genre in S3
    with TaskGroup('ValidateAndLoadArtistGenreFact') as PrepareArtistGenreFact:
        #Validate the artist fact
        validate_artist_genre=PythonOperator(
            task_id='ValidateArtistGenre',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_genre_fact') }}",
                'columns':['artist_id','genre'],
                "table": "artist_genre_bfact"
            },
            dag=dag
        )
        #Put the artist genre bridge in S3
        stage_artist_genre_bridge=PythonOperator(
            task_id='ArtistGenreBridgeToS3',
            python_callable=load_tables,
            op_kwargs={
                'key':today_str+'/artist_genre_fact.json',
                'bucketname':'spotify-top-50',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_genre_fact') }}"
            },
            dag=dag
        )
        validate_artist_genre >> stage_artist_genre_bridge

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
    finished = EmptyOperator(
        task_id = 'ETLDone',  
        dag = dag
    )


start_etl >> \
authenticate >> \
get_songs >> \
[artist_info, audio_quality] >> \
data_transformations >> \
[PrepareLoadSongFact,PrepareLoadSongDim,PrepareArtistFact, PrepareArtistGenreFact, PrepareSongArtistBridge ] >> \
json_csv >> \
finished



