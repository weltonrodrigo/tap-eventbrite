#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
import datetime
from tap_eventbrite.event import events_call
from tap_eventbrite.attendee import attendees_call
from tap_eventbrite.sales_report import sales_report_call
from tap_eventbrite.order import orders_call
from tap_eventbrite.category import categories_call
from tap_eventbrite.subcategory import subcategories_call

REQUIRED_CONFIG_KEYS = ["EVENTBRITE_TOKEN", "RUN_DAILY", "ORG_ID"]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def load_metadata(schema,key_properties=None,replication_keys=None):
    return [
            {
                "metadata":{
                    'replication-method':'INCREMENTAL',
                    'selected': True,
                    'schema-name':schema,
                    'valid-replication-keys': replication_keys,
                    'table-key-properties': key_properties,
                    "inclusion": "available",
                },
                "breadcrumb": []
            }
        ]

def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        stream_metadata = []

        if schema_name=="attendees":
            replication_keys = ['id']
            key_properties = ['id']

        if schema_name=="sales_reports":
            replication_keys = ['event_id', 'date']
            key_properties = ['event_id', 'date']

        if schema_name=="events":
            replication_keys = ['id']
            key_properties = ['id']

        if schema_name=="events":
            replication_keys = ['id']
            key_properties = ['id']

        if schema_name=="orders":
            replication_keys = ['id']
            key_properties = ['id']

        if schema_name=="categories":
            replication_keys = ['id']
            key_properties = ['id']

        if schema_name=="subcategories":
            replication_keys = ['id']
            key_properties = ['id']   

        stream_metadata = load_metadata(schema_name,key_properties,replication_keys)

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : stream_metadata,
            'key_properties': key_properties
        }

        streams.append(catalog_entry)

    return {'streams': streams}

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = metadata.to_map(stream['metadata'])
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def sync(config, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)
    loading_new_data = config['RUN_DAILY']
    events_list = []
    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        if stream_id in selected_stream_ids:
            # TODO: sync code for stream goes here...
            singer.write_schema(stream_id, stream_schema, stream['key_properties'])

            has_more_items = True
            continue_token = ""
            count = 0

            if stream_id == "events":
                
                # has_more_items = True
                # continue_token = ""

                while has_more_items:

                    records = events_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], continue_token)

                    if len(records['events'])==0:
                        LOGGER.info("{}: There is no data to stream".format(stream_id))

                    else: 
                        for record in records['events']:  
                            events_list.append(record['id'])
                            record = parse_date(stream_schema, record, loading_new_data)
                            if record is not None:
                                singer.write_record(stream_id, record)
                                count += 1
                    
                    if records['pagination'].get('continuation') is not None:
                        continue_token =  records['pagination'].get('continuation')
                    
                    else:
                        has_more_items = False

            elif stream_id == "attendees":
                # has_more_items = True
                # continue_token = ""

                while has_more_items:
                    changed_since = None

                    if loading_new_data:
                        changed_since = get_current_time_formatted()
                    
                    records = attendees_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], continue_token, changed_since)

                    # LOGGER.info(records)

                    if len(records['attendees']) == 0:
                        LOGGER.info("{}: There is no data to stream".format(stream_id))

                    else: 
                        for record in records['attendees']:  
                            record = parse_date(stream_schema, record)
                            if record is not None:
                                singer.write_record(stream_id, record)
                                count += 1
                    
                    if records['pagination'].get('continuation') is not None:
                        continue_token =  records['pagination'].get('continuation')
                    
                    else:
                        has_more_items = False

            elif stream_id == "sales_reports":
                LOGGER.info(events_list)
                LOGGER.info("Making loop for sales_reports. Times: {}!".format(len(events_list)))
                for event_id in events_list:
                    records = sales_report_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], event_id, loading_new_data)

                    if len(records['data']) == 0:
                        LOGGER.info("{}: There is no data to stream".format(stream_id))
                    
                    else:
                        for record in records['data']:  
                            record = parse_date(stream_schema, record)
                            record.update({"event_id": event_id})
                            # LOGGER.info(record)
                            if record is not None:
                                singer.write_record(stream_id, record)
                                count += 1

            elif stream_id == "orders":
                while has_more_items:
                    changed_since = None

                    if loading_new_data:
                        changed_since = get_current_time_formatted()
                    
                    records = orders_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], continue_token, changed_since)
                    
                    if len(records['orders']) == 0:
                        LOGGER.info("{}: There is no data to stream".format(stream_id))
                    
                    else:
                        for record in records['orders']:  
                            record = parse_date(stream_schema, record)
                            if record is not None:
                                singer.write_record(stream_id, record)
                                count += 1
                    if records['pagination'].get('continuation') is not None:
                        continue_token =  records['pagination'].get('continuation')
                    else:
                        has_more_items = False
            
            elif stream_id == "categories":

                while has_more_items:
                    if loading_new_data:
                        LOGGER.info("There is no data for categories!")
                        has_more_items = False

                    else:
                        records = categories_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], continue_token)
                        
                        if len(records['categories']) == 0:
                            LOGGER.info("{}: There is no data to stream".format(stream_id))
                        
                        else:
                            for record in records['categories']:  
                                record = parse_date(stream_schema, record)
                                if record is not None:
                                    singer.write_record(stream_id, record)
                                    count += 1

                        if records['pagination'].get('continuation') is not None:
                            continue_token =  records['pagination'].get('continuation')

                        else:
                            has_more_items = False

            elif stream_id == "subcategories":
    
                while has_more_items:
                    if loading_new_data:
                        LOGGER.info("There is no data for subcategories!")
                        has_more_items = False

                    else:
                        records = subcategories_call(config['EVENTBRITE_TOKEN'], config['ORG_ID'], continue_token)
                        if len(records['subcategories']) == 0:
                            LOGGER.info("{}: There is no data to stream".format(stream_id))
                        
                        else:
                            for record in records['subcategories']:  
                                record = parse_date(stream_schema, record)
                                if record is not None:
                                    singer.write_record(stream_id, record)
                                    count += 1

                        if records['pagination'].get('continuation') is not None:
                            continue_token =  records['pagination'].get('continuation')

                        else:
                            has_more_items = False

            else:
                LOGGER.info("Not match!")

            LOGGER.info('Syncing stream:' + stream_id)
            LOGGER.info("\033[92mFor {}: loaded {} record(s) into Stitch!\033[0m".format(stream_id, count))
    return

def get_current_time():
    now =  datetime.datetime.now() - datetime.timedelta(days=1)
    return now

def get_current_time_formatted():
    now =  datetime.datetime.now() - datetime.timedelta(days=1)
    formatted_date = "{}-{}-{}T00:00:00Z".format(now.year, now.month, now.day)
    return formatted_date

def parse_date(schema, record, loading_new_data = None):
    result = {}
    schema_properties = schema['properties']
    schema_keys = []

    if loading_new_data:
        created_time = datetime.datetime.strptime(record['created'],"%Y-%m-%dT%H:%M:%SZ")
        changed_time = datetime.datetime.strptime(record['created'],"%Y-%m-%dT%H:%M:%SZ")

        now =  get_current_time()
        changed_time_days = changed_time - now
        created_time_days = created_time - now

        if changed_time_days.days != 0 and created_time_days != 0:
            return None

    for schema_property in schema_properties:
        schema_keys.append(schema_property)

    for schema_key in schema_keys:
        schema_key_properties = schema_properties[schema_key]
        if "." in schema_key:
            dict_levels = schema_key.split('.')
            temp_data = record[dict_levels[0]]
            dict_levels.remove(dict_levels[0])

            for correct_schema in dict_levels:
                try:
                    temp_data = temp_data[correct_schema]
                    if schema_key_properties.get('format') is not None:
                        if "Z" not in temp_data:
                            temp_data = temp_data + "Z"

                except:
                    if schema_key_properties.get('format') is not None:
                        temp_data = "1971-01-01T00:00:00Z"
                    elif schema_key_properties['type'][1]  == "integer":
                        temp_data = 0
                    else:
                        temp_data = ""

            data = temp_data
            
        else:
            try:
                data = record[schema_key]
                
                if schema_key_properties.get('format') is not None:
                    if "Z" not in data:
                        data = data + "Z"

            except:
                if schema_key_properties.get('format') is not None:
                    data = "1971-01-01T00:00:00Z"
                elif schema_key_properties['type'][1]  == "integer":
                    data = 0
                else:
                    data = ""

        result.update({schema_key: data})

    return result

# Sorting catalog following the wish list
def sort_catalog(catalog):
    sorted_catalog = {'streams':[]}
    wish_list = ["events" ,"sales_reports" ,"attendees", "orders", "categories", "subcategories"]
    for element in wish_list:
        sorted_catalog['streams'].append({})
    
    for element in catalog['streams']:
        stream_id = element['stream']
        new_index = wish_list.index(stream_id)
        sorted_catalog['streams'][new_index] = element

    return sorted_catalog

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()

        sorted_catalog = sort_catalog(catalog)
        sync(args.config, args.state, sorted_catalog)

if __name__ == "__main__":
    main()
