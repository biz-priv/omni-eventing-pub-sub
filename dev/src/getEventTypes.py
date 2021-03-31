import boto3
import os
import json
import logging
client = boto3.client('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

InternalErrorMessage = "Internal Error."

def handler(event,context):
    logger.info("Event: {}".format(json.dumps(event)))
    try:
    success_message = {"message": "Successfully Executed"}
    return success_message