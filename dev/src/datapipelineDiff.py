import boto3
import os
import logging
client = boto3.client('redshift-data')
s3_client = boto3.client('s3')

def handler(event,context):
    logger.info("Event: {}".format(json.dumps(event)))
    try:
    success_message = {"message": "Successfully Executed"}
    return success_message