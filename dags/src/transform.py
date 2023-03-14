import pandas as pd
import time

#Method to transform all the playlist data into fact and dimensions tables so that it can be uploaded to our data warehouse
def transform_data(ti):

    #Get all the data frames we have collected

    #Get the songs_df which stores the following:
    #song_name, track_id, is_explicit, popularity, artist_name,artist_id
    songs_df=ti.xcom_pull(key='songs_df')
    songs_df=pd.read_json(songs_df)

    #Get the artists_df which stores the following:
    #artist_id, popularity, genre, followers
    artists_df=ti.xcom_pull(key='artists_df')
    artists_df=pd.read_json(artists_df)

    #Get the audio_df which stores the following:
    #id, acousticness, danceability, duration_ms, energy, instrumentalness, key, liveness, loudness, speechiness, tempo, valence
    audio_df=ti.xcom_pull(key='audio_df')
    audio_df=pd.read_json(audio_df)

    #Preform the transformation to make the needed fact and dimensiosn tables in our data model

    #Create the song fact table
    song_fact=songs_df.drop(['track_id','popularity'], axis=1)


    #Create the song dimension table
    song_dim=pd.merge(songs_df,audio_df,left_on='track_id',right_on='id', how='inner')
    song_dim=song_dim.drop(['popularity','id'], axis=1)

    #Create a song artist bridge table
    song_arist_bridge=songs_df[['track_id','artist_id']]
    song_arist_bridge.drop_duplicates(inplace=True)

    #Create the artist fact table 
    artist_fact=artists_df[['artist_id','popularity','followers']]

    #Create the artist dimension table 
    artist_dimension=pd.merge(songs_df,audio_df, left_on='artist_id',right_on='id',how='inner')
    artist_dimension=artist_dimension[['artist_name','id']]
    artist_dimension.drop_duplicates(inplace=True)

    #Create the artist genre bridge table 
    artist_genre_bridge=artists_df[['artist_id','genre']]
    artist_genre_bridge.drop_duplicates(inplace=True)

    #Create the Artist Genre Dimensions Table 
    genre=artists_df[['genre']]
    genre.drop_duplicates(inplace=True)

    #Push the dataframes into XCOM
    ti.xcom_push(key='song_fact', value=song_fact.to_json())
    ti.xcom_push(key='song_dim', value=song_dim.to_json())
    ti.xcom_push(key='song_arist_bridge', value=song_arist_bridge.to_json())
    ti.xcom_push(key='artist_fact', value=artist_fact.to_json())
    ti.xcom_push(key='artist_dimension', value=artist_dimension.to_json())
    ti.xcom_push(key='artist_genre_bridge', value=artist_genre_bridge.to_json())
    ti.xcom_push(key='genre', value=genre.to_json())






