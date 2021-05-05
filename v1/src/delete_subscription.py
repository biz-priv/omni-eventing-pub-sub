import os
import json
import logging
import validators
import jsonschema
import boto3
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
import pydash
from jsonschema import validate
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info("Event is : %s", json.dumps(event))
    customer_id = event['enhancedAuthContext']['customerId']
    validate_input(event['body'])

    event_type = event['body']['EventType']
    response = dynamo_get(customer_id, event_type)
    if len(response["Items"]) == 0:
        raise InputError(json.dumps({"httpStatus": 404, "message":"Subscription does not exist."}))

    try:
        sns_client.unsubscribe(SubscriptionArn=response['Items'][0]['Subscription_arn']['S'])
    except Exception as delete_sub_error:
        logging.exception("DeleteSubScriptionError: %s", delete_sub_error)
        raise DeleteSubScriptionError(json.dumps({"httpStatus": 400, "message": "Unable to delete subscription. Please contact admin for support."})) from delete_sub_error

    try:
        dynamo_delete(customer_id,event_type)
        success_message = {"message": "Unsubscribed successfully."}
        return success_message
    except Exception as delete_error:
        logging.exception("DeleteError: %s", delete_error)
        raise DeleteError(json.dumps({"httpStatus": 400, "message": "Unable to delete subscription. Please contact admin for support."})) from delete_error

def dynamo_get(customer_id, event_type):
    try:
        response = client.query(
                    TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
                    KeyConditionExpression='Customer_Id = :Customer_Id and Event_Type = :Event_Type',
                    ExpressionAttributeValues= {":Customer_Id": {"S": customer_id},
                                                ":Event_Type": {"S":event_type}},
                    ProjectionExpression='Subscription_arn')
        return response
    except Exception as get_error:
        logging.exception("DynamoGetError: %s", get_error)
        raise DynamoGetError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing subscription details"})) from get_error

def dynamo_delete(customer_id, event_type):
    try:
        client.delete_item(
            TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
            Key={'Customer_Id': {'S': customer_id},
                'Event_Type': {'S': event_type}})
    except Exception as delete_error:
        logging.exception("DynamoDeleteError: %s", delete_error)
        raise DynamoDeleteError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing subscription details"})) from delete_error

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
    except jsonschema.exceptions.ValidationError as val_error:
        raise InputError(json.dumps({"httpStatus": 400, "message" : val_error.message})) from val_error
    # if not validators.url(payload['Endpoint']) or not pydash.strings.starts_with(payload['Endpoint'],"https"):
    #     raise InputError(json.dumps({"httpStatus": 400, "message" : "Only Valid HTTPS endpoints are accepted"}))

class ValidationError(Exception):
    pass
class InputError(Exception):
    pass
class DynamoGetError(Exception):
    pass
class DeleteSubScriptionError(Exception):
    pass
class DynamoDeleteError(Exception):
    pass
class DeleteError(Exception):
    pass
