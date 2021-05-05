import os
import json
import boto3
client = boto3.client('stepfunctions')
import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):
    LOGGER.info("Event is : %s", json.dumps(event))
    response = client.start_execution(
        stateMachineArn=os.environ["SM_ARN"],
        input=json.dumps(event)
    )
    return json.dumps(response, default=str)
