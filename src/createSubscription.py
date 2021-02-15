import boto3
import os
import json
import logging
import botocore.session
session = botocore.session.get_session()
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


InternalErrorMessage = "Internal Error."

def handler(event, context):
    logger.info("Event body is: {}".format(json.dumps(event['body'])))
    customer_id = event['enhancedAuthContext']['customerId']
    customer_payload = event['body']
    preference = event['body']['Preference']
    event_type = event['body']['EventType']
    if "https" not in (event['body']['Endpoint']):
        return get_response("error", "Endpoint not accepted : Must be a only a https")
    else:
        response = dynamo_get(customer_id, event_type) 
        logger.info("Dynamo get response: {}".format(json.dumps(response)))
        
        if len(response["Items"]) != 0:
            return get_response("error", "Subscription already exists")
        else:
            update_customer_preference(customer_payload,customer_id)

        protocol = "https"            
        endpoint = event['body']['Endpoint']
        shared_secret = event['body']['SharedSecret']
        
        events_response = client.query(TableName=os.environ['EVENTING_TOPICS_TABLE'],
                                    KeyConditionExpression='Event_Type = :Event_Type', 
                                    ExpressionAttributeValues={':Event_Type': {'S': event_type}},
                                    ProjectionExpression='Event_Payload_Topic_Arn,Full_Payload_Topic_Arn')
        logger.info("Dynamo response from eventing table: {}".format(json.dumps(events_response)))

        if "full" in preference:
            full_topic_arn = events_response['Items'][0]['Full_Payload_Topic_Arn']['S']
            full_topic_response = subscribe_to_topic(full_topic_arn,protocol,endpoint,customer_id)
            return full_topic_response
        else:
            event_topic_arn = events_response['Items'][0]['Event_Payload_Topic_Arn']['S']
            event_topic_response = subscribe_to_topic(event_topic_arn,protocol,endpoint,customer_id)
            return event_topic_response


def subscribe_to_topic(topic_arn,protocol,endpoint,customer_id):
    try:
        response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol=protocol,
        Endpoint=endpoint,
         Attributes={"FilterPolicy": json.dumps({"customer_id": [customer_id]})},
        ReturnSubscriptionArn=True)
    except Exception as e:
        logging.exception("SubscribeToTopicError: {}".format(e))
        raise SubscribeToTopicError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))


def dynamo_get(customer_id, event_type):
    try:
        response = client.query(
                    TableName=os.environ['CUSTOMER_PREFERENCE_TABLE'],
                    KeyConditionExpression='Customer_Id = :Customer_Id and Event_Type = :Event_Type',
                    ExpressionAttributeValues= {":Customer_Id": {"S": customer_id}, 
                                                ":Event_Type": {"S":event_type}})
        return response
    except Exception as e:
        logging.exception("DynamoGetError: {}".format(e))
        raise DynamoGetError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))    

def update_customer_preference(customer_data,customer_id):
    try:
        event_type = customer_data['EventType']
        subscription_preference = customer_data['Preference']
        endpoint = customer_data['Endpoint']
        shared_secret = customer_data['SharedSecret']
        response = client.put_item(
            TableName = os.environ['CUSTOMER_PREFERENCE_TABLE'],
            Item={
                'Event_Type': {
                'S': event_type
                },
                'Subscription_Preference':{
                'S': subscription_preference
                },
                'Customer_Id': {
                'S': customer_id
                },
                'Endpoint' :{
                 'S': endpoint   
                },
                'Shared_Secret' :{
                 'S': shared_secret   
                }
            }
        )
        return response
    except Exception as e:
        logging.exception("UpdateCustomerPreferenceTableError: {}".format(e))
        raise UpdateCustomerPreferenceTableError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))


def get_response(status, msg):
    return {"status": status, "message": msg}

    
class UpdateCustomerPreferenceTableError(Exception): pass
class DynamoGetError(Exception): pass
class SubscribeToTopicError(Exception): pass