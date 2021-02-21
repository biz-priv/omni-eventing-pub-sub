import boto3
import os
import json
import logging
client = boto3.client('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

InternalErrorMessage = "Internal Error."

def handler(event,context):
    try:
        response = client.scan(TableName =os.environ['EVENTING_TOPICS_TABLE'],
                                AttributesToGet = ['Event_Type']) 
        event_records = {'Event Types': convert_event_types(response["Items"])}
        logger.info("All Event Types are: {}".format(json.dumps(event_records)))
        return event_records
    except Exception as e:
        logging.exception("GetEventsError: {}".format(e))
        raise GetEventsError(json.dumps({"httpStatus": 400, "message": e}))

def convert_event_types(event_types):
    try:
        events_list = []
        for items in event_types:
            events_list.append(items['Event_Type']['S'])
        return events_list
    except Exception as e:
        logging.exception("EventsConversionError: {}".format(e))
        raise EventsConversionError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))

class EventsConversionError(Exception): pass
class GetEventsError(Exception): pass