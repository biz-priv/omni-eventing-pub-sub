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
import pydash
import validators

INTERNAL_ERROR_MESSAGE = "Internal Error."

def handler(event, context):
    LOGGER.info("Event is: %s", json.dumps(event))
    customer_id = event['enhancedAuthContext']['customerId']    
    validate_input(event['body'])

    endpoint = event['body']['Endpoint']
    customer_payload = event['body']
    preference = event['body']['Preference']
    event_type = event['body']['EventType']

    response = dynamo_get(customer_id, event_type) 
    if len(response["Items"]) != 0:
        raise InputError(json.dumps({"httpStatus": 400, "message":"Subscription already exists"}))
        
    try:
        events_response = client.query(TableName=os.environ['EVENTING_TOPICS_TABLE'],
                                    KeyConditionExpression='Event_Type = :Event_Type', 
                                    ExpressionAttributeValues={':Event_Type': {'S': event_type}},
                                    ProjectionExpression='Event_Payload_Topic_Arn,Full_Payload_Topic_Arn')
    except Exception as events_error:
        logging.exception("EventingTopicsError: %s", json.dumps(events_error))
        raise EventingTopicsError(json.dumps({"httpStatus": 501, "message": INTERNAL_ERROR_MESSAGE})) from events_error

    if len(events_response["Items"]) == 0:
        raise InputError(json.dumps({"httpStatus": 400, "message":"EventType does not exists."}))

    try:
        if preference == "fullPayload":
            arn = events_response['Items'][0]['Full_Payload_Topic_Arn']['S']    
            response = subscribe_to_topic(arn,endpoint,customer_id)            
        else:
            arn = events_response['Items'][0]['Event_Payload_Topic_Arn']['S']
            response = subscribe_to_topic(arn,endpoint,customer_id)    
    except Exception as create_error:
        logging.exception("CreateSubScriptionError: %s", json.dumps(create_error))
        raise CreateSubScriptionError(json.dumps({"httpStatus": 501, "message": INTERNAL_ERROR_MESSAGE})) from create_error
        
    update_customer_preference(customer_payload,customer_id,response)    
    success_message = {"message": "Subscription successfully added"}
    return success_message

def subscribe_to_topic(topic_arn,endpoint,customer_id):
    try:
        response = sns_client.subscribe(TopicArn=topic_arn, Protocol="https",Endpoint=endpoint,
                                        Attributes={"FilterPolicy": json.dumps({"customer_id": [customer_id]})},
                                        ReturnSubscriptionArn=True)
        return response
    except Exception as subscribe_error:
        logging.exception("SubscribeToTopicError:  %s", json.dumps(subscribe_error))
        raise SubscribeToTopicError(json.dumps({"httpStatus": 501, "message": INTERNAL_ERROR_MESSAGE})) from subscribe_error

def dynamo_get(customer_id, event_type):
    try:
        response = client.query(
                    TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
                    KeyConditionExpression='Customer_Id = :Customer_Id and Event_Type = :Event_Type',
                    ExpressionAttributeValues= {":Customer_Id": {"S": customer_id}, 
                                                ":Event_Type": {"S":event_type}})
        return response
    except Exception as get_error:
        logging.exception("DynamoGetError:  %s", json.dumps(get_error))
        raise DynamoGetError(json.dumps({"httpStatus": 400, "message": "Unable to fetch existing subscription details"})) from get_error

def update_customer_preference(customer_data,customer_id,response):
    try:
        client.put_item(
            TableName = os.environ['CUSTOMER_PREFERENCE_TABLE'],
            Item={
                'Event_Type': {
                'S': customer_data['EventType']
                },
                'Subscription_Preference':{
                'S': customer_data['Preference']
                },
                'Customer_Id': {
                'S': customer_id
                },
                'Endpoint' :{
                 'S': customer_data['Endpoint']   
                },
                'Shared_Secret' :{
                 'S': customer_data['SharedSecret']   
                },
                'Subscription_arn': {
                'S': response['SubscriptionArn']
                }
            }
        )
    except Exception as update_error:
        logging.exception("UpdateCustomerPreferenceTableError: %s", json.dumps(get_error))
        raise UpdateCustomerPreferenceTableError(json.dumps({"httpStatus": 400, "message": e})) from update_error


def validate_input(payload):
    schema = {
    "type" : "object",
    "required": ["EventType",
                  "Endpoint",
                  "SharedSecret",
                  "Preference"],
    "properties" : {
        "EventType" : {"type" : "string"}, 
        "Endpoint" : {"type" : "string"},
        "SharedSecret" : {"type" : "string"},
        "Preference" : {"type" : "string",
                        "enum" :  ["fullPayload","Change"]}
        }}
    try:
        validate(instance=payload,schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise InputError(json.dumps({"httpStatus": 400, "message":e.message}))
    if not validators.url(payload['Endpoint']) or not pydash.strings.starts_with(payload['Endpoint'],"https"):
        raise InputError(json.dumps({"httpStatus": 400, "message":"Only Valid HTTPS endpoints are accepted"}))
    
        
class ValidationError(Exception):
    pass    
class InputError(Exception):
    pass    
class UpdateCustomerPreferenceTableError(Exception):
    pass
class DynamoGetError(Exception):
    pass
class EventingTopicsError(Exception):
    pass
class SubscribeToTopicError(Exception):
    pass
class CreateSubScriptionError(Exception):
    pass