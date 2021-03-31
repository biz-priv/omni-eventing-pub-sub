import os
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import botocore.session
session = botocore.session.get_session()

PolicyId="bizCloud|a1b2"
InternalErrorMessage="Internal Error."

def handler(event,context):
    logger.info("Event: {}".format(json.dumps(event)))
    try:
    success_message = {"message": "Successfully Executed"}
    return success_message