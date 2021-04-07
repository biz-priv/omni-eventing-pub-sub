import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event,context):
    logger.info("Event: {}".format(json.dumps(event)))
    success_message = {"message": "Successfully Executed"}
    return success_message