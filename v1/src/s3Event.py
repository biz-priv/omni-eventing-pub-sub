import os
import json
client = boto3.client('stepfunctions')
import boto3

def handler(event, context):
    response = client.start_execution(
        stateMachineArn=os.environ["SM_ARN"],
        input=json.dumps(event)
    )
    return json.dumps(response, default=str)