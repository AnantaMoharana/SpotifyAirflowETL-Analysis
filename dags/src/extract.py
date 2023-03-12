import requests
import pandas as pd

#Authenticates the Spotify API and gets the token
def authenitcate_api(ti):

    #The client ID and client secret from Spotify used for authenticating
    CLIENT_ID='bd44d89ed5704273a7a27786a2c86f35'
    CLIENT_SECRET='faa5559848ac466fb6173d2a6c8ff6be'

    #Spotify API Auth Url Endpoint
    AUTH_URL = 'https://accounts.spotify.com/api/token'

    # POST
    auth_response = requests.post(AUTH_URL, {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    })

    # convert the response to JSON
    auth_response_data = auth_response.json()

    # save the access token
    access_token = auth_response_data['access_token']

    print(access_token)

    ti.xcom_push(key='access_token', value=access_token)


#Get the songs present in the Top 50 Global Playlist https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF
def get_songs_from_playlist(ti):

    #Get the access token fron xcom
    access_token=ti.xcom_pull(key='access_token')

    #Headers for making the API call
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}

    #Base URL of all Spotify API endpoints
    BASE_URL = 'https://api.spotify.com/v1/'

    #Playlist ID from the URI
    playlist_id = '37i9dQZEVXbMDoHDwVN2tF'

    #print("This part takes too long")
    #GET request to get the playlist items from the endpoint
    r = requests.get(BASE_URL + 'playlists/' + playlist_id + '/tracks', headers=headers)

    #Convert the response to a json
    r = r.json()

    r

    #Create a dataframe to hold all the information
    songs_df = pd.DataFrame(columns=['song_name', 'track_id', 'is_explicit', 'popularity', 'artist_name','artist_id'])

    #Loop through the song items in the json
    for item in r['items']:
        #Get the song name
        song_name=item['track']['name']
        #Get the song id
        track_id=item['track']['id']
        #Get whether or not the song is explicit
        is_explicit=item['track']['explicit']
        #Get the songs popularity
        poplarity=item['track']['popularity']

        for artist in item['track']['artists']:
            #Get the artist name
            artist_name=artist['name']
            #Get the artist id
            artist_id=artist['id']
            #Create a new dataframe row and then append it
            new_row = pd.DataFrame({'song_name':song_name,'track_id':track_id,'is_explicit':is_explicit, 'popularity':poplarity, 'artist_name':artist_name,"artist_id":artist_id}, index=[0])
            songs_df = pd.concat([new_row,songs_df.loc[:]]).reset_index(drop=True)

    ti.xcom_push(key='songs_df', value=songs_df.to_json())

#Get the audio attributes of the songs
def get_artist_info(ti):
    #Get the songs_df which contains the airtist ids
    songs_df=ti.xcom_pull(key='songs_df')
    songs_df=pd.read_json(songs_df)

    #Get a list of all the artit ids and remove duplicates
    id_list=songs_df['artist_id'].tolist()
    id_list = list(set(id_list))

    #Get an access token to make the API calls
    access_token=ti.xcom_pull(key='access_token')

    #Headers for making the API call
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}

    #Base URL of all Spotify API endpoints
    BASE_URL = 'https://api.spotify.com/v1/'

    #Create a dataframe ot hold all the information about the artists
    artists_df = pd.DataFrame(columns=['artist_id', 'popularity', 'genre', 'followers']) 

    #Go through all the artist ids and make an api call to get their information
    for id in id_list:
        r = requests.get(BASE_URL + 'artists/' + id, headers=headers)
        r=r.json()
        #Get the total number of followers they have
        followers=r['followers']['total']
        #Get their popularity (0-100) the lower the more popular they are
        popularity=r['popularity']
        #Get all their potential genres
        genres=r['genres']
        for genre in genres:
            #add the new rows to the dataframe 
            new_row = pd.DataFrame({'artist_id':id,'popularity':popularity,'genre':genre, 'followers':followers}, index=[0])
            artists_df = pd.concat([new_row,artists_df.loc[:]]).reset_index(drop=True)

    ti.xcom_push(key='artists_df',value=artists_df.to_json())

def get_song_audio_quality(ti):

    #get the songs_df which contains the song ids of all the songs in the playlist
    songs_df=ti.xcom_pull(key='songs_df')
    songs_df=pd.read_json(songs_df)

    #Get a list fo all the unique ids and remove the duplicates
    song_ids=songs_df['track_id'].tolist()
    song_ids=list(set(song_ids))

    #Get an access token to make the API calls
    access_token=ti.xcom_pull(key='access_token')

    #Headers for making the API call
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}

    #Base URL of all Spotify API endpoints
    BASE_URL = 'https://api.spotify.com/v1/'

    #Create a dataframe ot hold all the information about the song audio
    audio_df = pd.DataFrame(columns=['id','acousticness','danceability','duration_ms', 'energy', 'instrumentalness','key', 'liveness', 'loudness', 'speechiness','tempo', 'valence']) 
    for id in song_ids:
        #Get the release date of the song
        r=requests.get(BASE_URL + 'audio-features/' + id, headers=headers)
        r=r.json()
        #Get acousticness
        acousticness=r['acousticness']
        #Get danceability
        danceability=r['danceability']
        #Get duration_ms
        duration_ms=r['duration_ms']
        #Get energy
        energy=r['energy']
        #Get instrumentalness
        instrumentalness=r['instrumentalness']
        #Get key
        key=r['key']
        #Get liveness
        liveness=r['liveness']
        #Get loudness
        loudness=r['loudness']
        #Get speechiness
        speechiness=r['speechiness']
        #Get tempo
        tempo=r['tempo']
        #Get valence
        valence=r['valence']
        #Create a new row
        new_row = pd.DataFrame({'id':id,'acousticness':acousticness,'danceability':danceability,'duration_ms':duration_ms, 'energy':energy, 'instrumentalness':instrumentalness,'key':key, 'liveness':liveness, 'loudness':loudness, 'speechiness':speechiness,'tempo':tempo, 'valence':valence }, index=[0])
        audio_df = pd.concat([new_row,audio_df.loc[:]]).reset_index(drop=True)

    ti.xcom_push(key='audio_df',value=audio_df.to_json())

