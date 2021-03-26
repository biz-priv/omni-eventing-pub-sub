import os
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import botocore.session
session = botocore.session.get_session()

PolicyId="bizCloud|a1b2"
InternalErrorMessage="Internal Error."

def generate_policy(principal_id, effect, method_arn, customer_id = None, message = None):
    try:
        print ("Inserting "+effect+" policy on API Gateway")
        policy = {}
        policy["principalId"] = principal_id
        policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'ApiAccess',
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': method_arn
                }
            ]
        }
        policy["policyDocument"] = policy_document
        if message:
            policy["context"] = {"message": message}
        else:
            if customer_id:
                policy["context"] = {"customerId": customer_id}
        logger.info("Policy: {}".format(json.dumps(policy)))
        return policy
    except Exception as e:
        logging.exception("GeneratePolicyError: {}".format(e))
        raise GeneratePolicyError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))

def handler(event, context):
    try:        
        logger.info("Event: {}".format(json.dumps(event)))
        api_key = event['headers']['x-api-key']
    except Exception as e:
        logging.exception("ApiKeyError: {}".format(e))
        raise ApiKeyError(json.dumps({"httpStatus": 400, "message": "API Key not passed."}))

    response = dynamo_query(os.environ["TOKEN_VALIDATION_TABLE"], os.environ["TOKEN_VALIDATION_TABLE_INDEX"], 
            'ApiKey = :apikey', {":apikey": {"S": api_key}})
    
    customer_id = validate_dynamo_query_response(response, event, None, "Customer Id not found.")
    
    if "POST/webhook" in event["methodArn"]:
        return generate_policy("postPolicyId123", 'Allow', event["methodArn"], customer_id)
    elif "DELETE/webhook" in event["methodArn"]:
        return generate_policy("deletePolicyId456", 'Allow', event["methodArn"], customer_id)
    if "GET/events" in event["methodArn"]:
        return generate_policy(PolicyId, 'Allow', event["methodArn"], customer_id)
    if customer_id != "admin" and "POST/events" in event["methodArn"]:
        return generate_policy(PolicyId, 'Deny', event["methodArn"], customer_id, message = "API can only be accessed by admins. Contact support and request admin credentials.")
    else:
        return generate_policy(PolicyId, 'Allow', event["methodArn"], customer_id)

def validate_dynamo_query_response(response, event, customer_id=None, message=None):
    try:
        if not response or "Items" not in response or len(response['Items']) == 0:
            return generate_policy(None, 'Deny', event["methodArn"], None, message)
        if not customer_id:
            return response['Items'][0]['CustomerID']['S']
        else:
            return generate_policy(PolicyId, 'Allow', event["methodArn"], customer_id)
    except Exception as e:
        logging.exception("CustomerIdNotFound: {}".format(e))
        raise CustomerIdNotFound(json.dumps({"httpStatus": 400, "message": "Customer Id not found."}))

def dynamo_query(table_name, index_name, expression, attributes):
    try:
        client = session.create_client('dynamodb', region_name=os.environ['REGION'])
        response = client.query(
            TableName=table_name,
            IndexName=index_name,
            KeyConditionExpression=expression,
            ExpressionAttributeValues=attributes
        )
        logger.info("Dynamo query response: {}".format(json.dumps(response)))
        return response
    except Exception as e:
        logging.exception("DynamoQueryError: {}".format(e))
        raise DynamoQueryError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))

def get_response(status, msg):
    return {"status": status, "message": msg}

class ApiKeyError(Exception): pass
class HandlerError(Exception): pass
class CustomerIdNotFound(Exception): pass
class GeneratePolicyError(Exception): pass
class InputError(Exception): pass
class DynamoQueryError(Exception): pass
