import os
import json
import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
import boto3
import jsonschema
from jsonschema import validate
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')

def handler(event, context):
    LOGGER.info("Event: %s", json.dumps(event))

    validate_input(event['body'])
    event_type = event['body']['EventType']

    response = dynamo_get(event_type)
    if not "Item" in response:
        topics_arns = create_topic(event_type)
        insert_eventing_topics(event_type, topics_arns)
        success_message = {"message": "Topic creation Successful"}
        return success_message
    if len(response["Item"]) != 0:
        raise InputError(json.dumps({"httpStatus": 409, "message":"Event type already exists."}))

def create_topic(event_type):
    try:
        full_response = sns_client.create_topic(Name=event_type+"_fullPayload")
        change_response = sns_client.create_topic(Name=event_type+"_change")
        topics_arns = [full_response["TopicArn"], change_response["TopicArn"]]
        return topics_arns
    except Exception as create_error:
        logging.exception("TopicCreationError: %s", json.dumps(create_error))
        raise TopicCreationError(json.dumps({"httpStatus": 400, "message": "Unable to create Event Type"})) from create_error

def insert_eventing_topics(event_type,topics_arns):
    try:
        client.put_item(
            TableName = os.environ['EVENTING_TOPICS_TABLE'],
            Item={
                'Event_Type': {
                'S': event_type
                },
                'Event_Payload_Topic_Arn':{
                'S': topics_arns[1]
                },
                'Event_Payload_Topic_Name': {
                'S': event_type+"_change"
                },
                'Full_Payload_Topic_Arn' :{
                 'S': topics_arns[0]
                },
                'Full_Payload_Topic_Name' :{
                 'S': event_type+"_fullPayload"
                }
            }
        )
    except Exception as insert_error:
        logging.exception("InsertEventingTopicsError: %s", json.dumps(insert_error))
        raise InsertEventingTopicsError(json.dumps({"httpStatus": 400, "message": "Unable to create Event Type"})) from insert_error

def dynamo_get(event_type):
    try:
        response = client.get_item(TableName=os.environ['EVENTING_TOPICS_TABLE'],
                                Key={'Event_Type':{'S':event_type}})
        return response
    except Exception as get_error:
        logging.exception("DynamoGetError: %s", json.dumps(get_error))
        raise DynamoGetError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing Event Types"})) from get_error

def validate_input(event):
    schema = {
    "type" : "object",
    "required": ["EventType"],
    "properties" : {"EventType" : {"type" : "string"}}}
    try:
        validate(instance=event,schema=schema)
    except jsonschema.exceptions.ValidationError as validation_error:
        raise ValidationError(json.dumps({"httpStatus": 400, "message":validation_error.message})) from validation_error

class InputError(Exception):
    pass
class DynamoGetError(Exception):
    pass
class TopicCreationError(Exception):
    pass
class InsertEventingTopicsError(Exception):
    pass
class ValidationError(Exception):
    pass
