import requests
import time
import os
import singer
import json  

LOGGER = singer.get_logger() 

def attendees_call(token, org, continue_token, changed_since):
    if changed_since is not None:
        date = changed_since
    else:
        date = "2020-01-01T00:00:00Z"

    url = "https://www.eventbriteapi.com/v3/organizations/{}/attendees/?changed_since={}".format(org ,date)

    if len(continue_token) > 0:
        url = url + "&continuation={}".format(continue_token)
    
    headers = {
        'authorization': "Bearer {}".format(token)
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        attendees_json = response.json()
        return attendees_json

    else:
        LOGGER.info("An error occerred when calling Attendees API!")
        return None
