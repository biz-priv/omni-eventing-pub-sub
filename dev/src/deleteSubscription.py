import boto3
import os
import json
import logging
import pydash
import validators
import jsonschema
from jsonschema import validate
client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

InternalErrorMessage = "Internal Error."

def handler(event,context):
    logger.info("Event: {}".format(json.dumps(event)))
    try:
    success_message = {"message": "Successfully Executed"}
    return success_message