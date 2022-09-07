import os
import json
import boto3
import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
client = boto3.client('stepfunctions')


def handler(event, context):
    LOGGER.info("Event is: %s", json.dumps(event))
    response = client.start_execution(
        stateMachineArn=os.environ["SM_ARN"],
        input=json.dumps(event)
    )
    LOGGER.info("Response is: %s", response)
    return json.dumps(response, default=str)
