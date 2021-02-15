import requests
import time
import os
import singer
import json  

LOGGER = singer.get_logger() 

def events_call(token, org, continue_token):
    url = "https://www.eventbriteapi.com/v3/organizations/{}/events/?page_size=100".format(org)

    if len(continue_token) > 0:
        url = url + "&continuation={}".format(continue_token)

    headers = {
        'authorization': "Bearer {}".format(token)
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        event_json = response.json()
        return event_json

    else:
        LOGGER.info("An error occurred when calling Events API!")
        return None
