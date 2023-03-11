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

    ti.xcom_push(key='access_token', value=access_token)


