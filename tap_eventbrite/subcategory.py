import requests
import time
import os
import singer
import json  

LOGGER = singer.get_logger() 

def subcategories_call(token, org, continue_token):
    url = "https://www.eventbriteapi.com/v3/subcategories/"

    if len(continue_token) > 0:
        url = url + "?continuation={}".format(continue_token)
        
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
