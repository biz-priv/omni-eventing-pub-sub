import os
import json
import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
import botocore.session
session = botocore.session.get_session()

POLICY_ID="bizCloud|a1b2"
INTERNAL_ERROR_MESSAGE="Internal Error."

def generate_policy(principal_id, effect, customer_id = None, message = None):
    try:
        LOGGER.info("Inserting %s policy on API Gateway", effect)
        policy = {}
        policy["principalId"] = principal_id
        policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'ApiAccess',
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': "*"
                }
            ]
        }
        policy["policyDocument"] = policy_document
        if message:
            policy["context"] = {"message": message}
        else:
            if customer_id:
                policy["context"] = {"customerId": customer_id}
        LOGGER.info("Policy: %s", json.dumps(policy))
        return policy
    except Exception as policy_error:
        logging.exception("GeneratePolicyError: %s", json.dumps(policy_error))
        raise GeneratePolicyError(json.dumps({"httpStatus": 501, "message": INTERNAL_ERROR_MESSAGE})) from policy_error

def handler(event, context):
    try:
        LOGGER.info("Event is: %s", json.dumps(event))
        api_key = event['headers']['x-api-key']
    except Exception as key_error:
        logging.exception("ApiKeyError: %s", json.dumps(key_error))
        raise ApiKeyError(json.dumps({"httpStatus": 400, "message": "API Key not passed."})) from key_error

    response = dynamo_query(os.environ["TOKEN_VALIDATION_TABLE"], os.environ["TOKEN_VALIDATION_TABLE_INDEX"],
            'ApiKey = :apikey', {":apikey": {"S": api_key}})

    customer_id = validate_dynamo_query_response(response, event, None, "Customer Id not found.")

    if "POST/webhook" in event["methodArn"]:
        return generate_policy("postPolicyId123", 'Allow', customer_id)
    if "DELETE/webhook" in event["methodArn"]:
        return generate_policy("deletePolicyId456", 'Allow',  customer_id)
    if "GET/events" in event["methodArn"]:
        return generate_policy(POLICY_ID, 'Allow',  customer_id)
    if customer_id != "admin" and "POST/events" in event["methodArn"]:
        return generate_policy(POLICY_ID, 'Deny', customer_id, message = "API can only be accessed by admins. Contact support and request admin credentials.")
    # elif:
    return generate_policy(POLICY_ID, 'Allow', customer_id)

def validate_dynamo_query_response(response, event, customer_id=None, message=None):
    try:
        if not response or "Items" not in response or len(response['Items']) == 0:
            return generate_policy(None, 'Deny', event["methodArn"], None, message)
        if not customer_id:
            return response['Items'][0]['CustomerID']['S']
        # else:
        return generate_policy(POLICY_ID, 'Allow', event["methodArn"], customer_id)
    except Exception as id_error:
        logging.exception("CustomerIdNotFound: %s", json.dumps(id_error))
        raise CustomerIdNotFound(json.dumps({"httpStatus": 400, "message": "Customer Id not found."})) from id_error

def dynamo_query(table_name, index_name, expression, attributes):
    try:
        client = session.create_client('dynamodb', region_name=os.environ['REGION'])
        response = client.query(
            TableName=table_name,
            IndexName=index_name,
            KeyConditionExpression=expression,
            ExpressionAttributeValues=attributes
        )
        return response
    except Exception as dynamo_error:
        logging.exception("DynamoQueryError: %s", json.dumps(dynamo_error))
        raise DynamoQueryError(json.dumps({"httpStatus": 501, "message": INTERNAL_ERROR_MESSAGE})) from dynamo_error

class ApiKeyError(Exception):
    pass
class HandlerError(Exception):
    pass
class CustomerIdNotFound(Exception):
    pass
class GeneratePolicyError(Exception):
    pass
class InputError(Exception):
    pass
class DynamoQueryError(Exception):
    pass
