import boto3
import os
import json
import logging
import jsonschema
from jsonschema import validate
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

InternalErrorMessage = "Internal Error."

def handler(event, context):
    logger.info("Event body is: {}".format(json.dumps(event['body'])))

    validate_input(event['body'])
    event_type = event['body']['EventType']
 
    response = dynamo_get(event_type) 
    logger.info("Dynamo get response: {}".format(json.dumps(response)))
    if not "Item" in response:
        topics_arns = create_topic(event_type)
        insert_eventing_topics(event_type,topics_arns)
        success_message = {"message": "Topic creation Successful"}
        return success_message
    if len(response["Item"]) != 0:
        raise InputError(json.dumps({"httpStatus": 409, "message":"Event type already exists."}))
    
def create_topic(event_type):
    try:
        full_response = sns_client.create_topic(Name=event_type+"_fullPayload")
        change_response = sns_client.create_topic(Name=event_type+"_change")
        topics_arns = [full_response["TopicArn"],change_response["TopicArn"]]
        return topics_arns
    except Exception as e:
        logging.exception("TopicCreationError: {}".format(e))
        raise TopicCreationError(json.dumps({"httpStatus": 400, "message": "Unable to create Topic"}))

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
    except Exception as e:
        logging.exception("InsertEventingTopicsError: {}".format(e))
        raise InsertEventingTopicsError(json.dumps({"httpStatus": 400, "message": e}))

def dynamo_get(event_type):
    try:
        response = client.get_item(TableName=os.environ['EVENTING_TOPICS_TABLE'],
                                Key={'Event_Type':{'S':event_type}})                                                    
        return response
    except Exception as e:
        logging.exception("DynamoGetError: {}".format(e))
        raise DynamoGetError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing Event Types"}))

def validate_input(event):
    schema = {
    "type" : "object",
    "required": ["EventType"],  
    "properties" : {"EventType" : {"type" : "string"}}}
    try:
        validate(instance=event,schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise ValidationError(json.dumps({"httpStatus": 400, "message":e.message}))

class InputError(Exception): pass    
class DynamoGetError(Exception): pass
class TopicCreationError(Exception): pass
class InsertEventingTopicsError(Exception): pass
class snsCreationError(Exception): pass
class ValidationError(Exception): pass