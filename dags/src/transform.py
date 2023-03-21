import pandas as pd
import time
from datetime import datetime, timedelta
#Method to transform all the playlist data into fact and dimensions tables so that it can be uploaded to our data warehouse
def transform_data(ti):

    #Get all the data frames we have collected

    #Get the songs_df which stores the following:
    #song_name, song_id, is_explicit, popularity, artist_name,artist_id
    songs_df=ti.xcom_pull(key='songs_df')
    songs_df=pd.read_json(songs_df)

    #Get the artists_df which stores the following:
    #artist_id, popularity, genre, followers
    artists_df=ti.xcom_pull(key='artists_df')
    artists_df=pd.read_json(artists_df)

    #Get the audio_df which stores the following:
    #id, acousticness, danceability, duration_ms, energy, instrumentalness, key, liveness, loudness, speechiness, tempo, valence, mode
    audio_df=ti.xcom_pull(key='audio_df')
    audio_df=pd.read_json(audio_df)

    #Preform the transformation to make the needed fact and dimensiosn tables in our data model

    #Create the song fact table
    song_fact=songs_df[['song_id','popularity']]
    today = datetime.today().strftime('%Y-%m-%d')
    song_fact['date']=today
    song_fact.drop_duplicates(inplace=True)

    #Create the song dimension table
    song_dim=pd.merge(songs_df,audio_df,left_on='song_id',right_on='song_id', how='inner')
    song_dim=song_dim.drop(['popularity','artist_name','artist_id'], axis=1)
    song_dim.drop_duplicates(inplace=True)

    #Create the artist dimension table
    artist_dim=pd.merge(songs_df,audio_df,left_on='song_id',right_on='song_id', how='inner')
    artist_dim=artist_dim[['artist_name','artist_id']]
    artist_dim.drop_duplicates(inplace=True)

    

    #Create a song artist bridge table
    song_artist_bridge=songs_df[['song_id','artist_id']]
    song_artist_bridge.drop_duplicates(inplace=True)

    #Create the artist fact table 
    artist_fact=pd.merge(songs_df,artists_df, left_on='artist_id', right_on='artist_id', suffixes=('_left', '_right'))
    artist_fact=artist_fact[['artist_id','followers','popularity_right']]
    artist_fact = artist_fact.rename(columns={'popularity_right': 'popularity'})
    artist_fact['date']=today
    artist_fact.drop_duplicates(inplace=True)

    #Create the artist genre bridge table 
    artist_genre_fact=artists_df[['artist_id','genre']]
    artist_genre_fact.drop_duplicates(inplace=True)


    #Push the dataframes into XCOM
    ti.xcom_push(key='song_fact', value=song_fact.to_json())
    ti.xcom_push(key='song_dim', value=song_dim.to_json())
    ti.xcom_push(key='song_artist_bridge', value=song_artist_bridge.to_json())
    ti.xcom_push(key='artist_fact', value=artist_fact.to_json())
    ti.xcom_push(key='artist_genre_fact', value=artist_genre_fact.to_json())
    ti.xcom_push(key='artist_dim', value=artist_dim.to_json())

#Implement  data validation methods to that we can vlaidate the data
def validate_data(data,columns,table):

    print(data)
    print(columns)
    print(table)
    data_table=pd.read_json(data)
    print(data_table)

    #Check if all rows have unique values 
    if len(data_table.drop_duplicates()) != len(data_table):
        raise ValueError("{} Contains Duplicate Values".format(table))
    #Check if all the colmuns match
    if list(data_table.columns) != columns:
        print(data_table.columns)
        print(columns)
        raise ValueError("{} doesn't contain the correct rows".format(table)) 
        
    #Check to be sure we have no nulls
    if data_table.isna().sum().sum() > 0:
        raise ValueError("{} Contains Null Values".format(table))
