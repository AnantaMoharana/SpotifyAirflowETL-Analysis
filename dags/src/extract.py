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
