import os
import json
import logging
import boto3
client = boto3.client('dynamodb')

def handler(event,context):
    try:
        response = client.scan(TableName =os.environ['EVENTING_TOPICS_TABLE'],
                                AttributesToGet = ['Event_Type'])
        event_records = {'Event Types': convert_event_types(response["Items"])}
        return event_records
    except Exception as get_error:
        logging.exception("GetEventsError: %s", get_error)
        raise GetEventsError(json.dumps({"httpStatus" : 400, "message" : "Unable to fetch existing Event Types"})) from get_error

def convert_event_types(event_types):
    try:
        events_list = []
        for items in event_types:
            events_list.append(items['Event_Type']['S'])
        return events_list
    except Exception as convert_error:
        logging.exception("EventsConversionError: %s", convert_error)
        raise EventsConversionError(json.dumps({"httpStatus": 400, "message": "Error while converting Event Types"})) from convert_error

class EventsConversionError(Exception):
    pass
class GetEventsError(Exception):
    pass
