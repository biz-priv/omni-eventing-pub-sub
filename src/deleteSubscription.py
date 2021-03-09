import boto3
import os
import json
import logging
import pydash
import validators
import jsonschema
from jsonschema import validate
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

InternalErrorMessage = "Internal Error."

def handler(event, context):
    logger.info("Event is: {}".format(json.dumps(event)))

    customer_id = event['enhancedAuthContext']['customerId']    
    validate_input(event['body'])

    event_type = event['body']['EventType']
    
    response = dynamo_get(customer_id, event_type) 
    logger.info("Dynamo get response: {}".format(json.dumps(response)))
    
    if len(response["Items"]) == 0:
        raise InputError(json.dumps({"httpStatus": 404, "message":"Subscription does not exist."}))
    
    try:
        sns_client.unsubscribe(SubscriptionArn=response['Items'][0]['Subscription_arn']['S'])            
        success_message = {"message": "Unsubscribed successfully."}
        return success_message
    except Exception as e:
        logging.exception("DeleteSubScriptionError: {}".format(e))
        raise DeleteSubScriptionError(json.dumps({"httpStatus": 400, "message": "Unable to delete subscription. Please contact admin for support."}))

    try:
        dynamo_delete(customer_id,event_type)
    except Exception as e:
        logging.exception("DeleteError: {}".format(e))
        raise DeleteError(json.dumps({"httpStatus": 400, "message": "Unable to delete subscription. Please contact admin for support."}))    

def dynamo_get(customer_id, event_type):
    try:
        response = client.query(
                    TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
                    KeyConditionExpression='Customer_Id = :Customer_Id and Event_Type = :Event_Type',
                    ExpressionAttributeValues= {":Customer_Id": {"S": customer_id}, 
                                                ":Event_Type": {"S":event_type}},
                    ProjectionExpression='Subscription_arn')
        return response
    except Exception as e:
        logging.exception("DynamoGetError: {}".format(e))
        raise DynamoGetError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing subscription details"}))    


def dynamo_delete(customer_id, event_type):
    try:
        client.delete_item(
            TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
            KeyConditionExpression='Customer_Id = :Customer_Id and Event_Type = :Event_Type',
            ExpressionAttributeValues= {":Customer_Id": {"S": customer_id}, 
                                        ":Event_Type": {"S":event_type}})
    except Exception as e:
        logging.exception("DynamoDeleteError: {}".format(e))
        raise DynamoDeleteError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing subscription details"}))    


def validate_input(payload):
    schema = {
    "type" : "object",
    "required": ["EventType",
                  "Endpoint"],
    "properties" : {
        "EventType" : {"type" : "string"}, 
        "Endpoint" : {"type" : "string"}}
        }
    try:
        validate(instance=payload,schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise InputError(json.dumps({"httpStatus": 400, "message":e.message}))
    if not validators.url(payload['Endpoint']) or not pydash.strings.starts_with(payload['Endpoint'],"https"):
        raise InputError(json.dumps({"httpStatus": 400, "message":"Only Valid HTTPS endpoints are accepted"}))
    
        
class ValidationError(Exception): pass    
class InputError(Exception): pass    
class DynamoGetError(Exception): pass
class DeleteSubScriptionError(Exception): pass
class DynamoDeleteError(Exception): pass
class DeleteError(Exception): pass