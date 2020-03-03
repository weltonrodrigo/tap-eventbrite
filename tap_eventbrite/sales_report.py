import requests
import time
import os
import singer
import json  

LOGGER = singer.get_logger() 

def sales_report_call(token, org, event_id, loading_new_data):
    url = "https://www.eventbriteapi.com/v3/reports/sales/?event_ids={}".format(event_id)

    if loading_new_data:
        url = url + "&date_facet=day&period=1"

    headers = {
        'authorization': "Bearer {}".format(token)
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        return response.json()
    
    else:
        LOGGER.info("An error occerred when calling Sales Report API!")
        return None
