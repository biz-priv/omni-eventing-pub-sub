import json
import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event):
    LOGGER.info("Event: %s", json.dumps(event))
    success_message = {"message": "Successfully Executed"}
    return success_message
