import boto3
import json
import os
client = boto3.client('stepfunctions')

def handler(event, context):
    response = client.start_execution(
        stateMachineArn=os.environ["SM_ARN"],
        input=json.dumps(event)
    )
    return json.dumps(response, default=str)