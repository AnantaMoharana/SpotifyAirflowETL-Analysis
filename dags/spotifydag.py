#import libraries
import datetime
from datetime import datetime, timedelta, date
import pandas as pd
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from src.extract import authenitcate_api, get_songs_from_playlist, get_artist_info, get_song_audio_quality
from src.transform import transform_data, validate_data
from src.load import load_tables, json_to_csv

#Get todays date
today_str = datetime.today().strftime('%Y-%m-%d')
month=datetime.today().month
month_name=datetime.today().strftime("%B")
day_of_month=datetime.today().day
day_of_week=datetime.today().weekday()
day_name=datetime.today().strftime("%A")
quarter=(month -1) // 3 +1

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
                'columns':['song_id','popularity','date'],
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
                'columns':['song_name','song_id','explicit','acousticness', 'danceability','duration_ms','energy','instrumentalness','key','liveness','loudness','speechiness','tempo','valence','mode'],
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
        validate_song_artist_bridge=PythonOperator(
            task_id='ValidateSongArtistBridge',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}",
                'columns':['song_id','artist_id'],
                "table": "song_artist_bridge"
            },
            dag=dag
        )
        #Put the song artist brudge in S3
        stage_song_artist_bridge=PythonOperator(
            task_id='SongArtistBridgeToS3',
            python_callable=load_tables,
            op_kwargs={
                'key':today_str+'/song_artist_bridge.json',
                'bucketname':'spotify-top-50',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='song_artist_bridge') }}"
            },
            dag=dag
        )
        validate_song_artist_bridge >> stage_song_artist_bridge

    #Validate and put Artist Fact in S3
    with TaskGroup('ValidateAndLoadArtistFact') as PrepareArtistFact:
        #Validate the artist fact
        validate_artist_fact=PythonOperator(
            task_id='ValidateArtistFact',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_fact') }}",
                'columns':['artist_id','followers','popularity','date'],
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

    #Validate and put Artist Genre in S3
    with TaskGroup('ValidateAndLoadArtistDim') as PrepareArtistDim:
        #Validate the artist dim
        validate_artist_dim=PythonOperator(
            task_id='ValidateArtistDim',
            python_callable=validate_data,
            op_kwargs={
                'data':"{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_dim') }}",
                'columns':['artist_name','artist_id'],
                "table": "artist_dim"
            },
            dag=dag
        )
        #Put the artist dim  in S3
        stage_artist_dim=PythonOperator(
            task_id='ArtistDimToS3',
            python_callable=load_tables,
            op_kwargs={
                'key':today_str+'/artist_dim.json',
                'bucketname':'spotify-top-50',
                "data": "{{ task_instance.xcom_pull(task_ids='TransformData', key='artist_dim') }}"
            },
            dag=dag
        )
        validate_artist_dim >> stage_artist_dim



    #Convert the jsons to CSV
    json_csv=PythonOperator(
        task_id='JsonToCSV',
        python_callable=json_to_csv,
        op_kwargs={
            'pay_load':{"s3_bucket":"spotify-top-50", "s3_folder":today_str}
        },
        dag=dag
    )

    #Move the data into the datawarehosue
    with TaskGroup('StoreDataInWarehouse') as StageDataInWarehouse:
        #Load the SongDim Table into Snowflake
        SongDim_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreSongDim',
            s3_keys=['{}/song_dim.csv'.format(today_str)],
            table='song_dim',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )

        #Load the ArtistDim Table into Snowflake
        ArtistDim_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreArtistDim',
            s3_keys=['{}/artist_dim.csv'.format(today_str)],
            table='artist_dim',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )
        #Load the calendar dim into Snowflake
        insert_date=SnowflakeOperator(
        task_id='insert_date',
        sql="""
        INSERT INTO calendar_dim (date, month, month_name,day_of_month,day_of_week,day_name,quarter)
        VALUES ('{date}', {month}, '{month_name}',{day_of_month},{day_of_week},'{day_name}',{quarter})
        """.format(date=today_str,month=month,month_name=month_name,day_of_month=day_of_month,day_of_week=day_of_week,day_name=day_name,quarter=quarter),
        snowflake_conn_id='snowflake_connection',
        database='top50usa',
        schema='SpotifyTop50USA',
        warehouse='SPOTIFYTOP50',
        role="ACCOUNTADMIN"
        )

        #Load the ArtistDim Table into Snowflake
        SongArtistBridge_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreSongArtistBridge',
            s3_keys=['{}/song_artist_bridge.csv'.format(today_str)],
            table='song_artist',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )
        #Load the SongFact Table into Snowflake
        SongFact_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreSongFact',
            s3_keys=['{}/song_fact.csv'.format(today_str)],
            table='song_fact',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )

        #Load the ArtistDim Table into Snowflake
        ArtistGenreFact_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreArtistGenre_to_snowflake',
            s3_keys=['{}/artist_genre_fact.csv'.format(today_str)],
            table='artist_dim',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )

        #Load the ArtistPopularityFact Table into Snowflake
        ArtistPopularityFact_to_snowflake = S3ToSnowflakeOperator(
            task_id='StoreArtistPopularityFact_to_snowflake',
            s3_keys=['{}/artist_fact.csv'.format(today_str)],
            table='artist_popularity_fact',
            file_format="DATA_CSV",
            schema = "SpotifyTop50USA",
            warehouse = "SPOTIFYTOP50",
            database = "top50usa",
            snowflake_conn_id = 'snowflake_connection' ,
            stage='s3_snowflake_stage',
            role = "ACCOUNTADMIN"

        )
        SongDim_to_snowflake >> ArtistDim_to_snowflake >> insert_date >> SongArtistBridge_to_snowflake >> SongFact_to_snowflake >> ArtistGenreFact_to_snowflake >> ArtistPopularityFact_to_snowflake

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
[PrepareLoadSongFact,PrepareLoadSongDim,PrepareArtistFact, PrepareArtistGenreFact, PrepareSongArtistBridge,PrepareArtistDim ] >>\
json_csv >> \
StageDataInWarehouse >>\
finished



