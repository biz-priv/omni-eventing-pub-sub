import os
import json
import logging
import boto3
client = boto3.client('redshift-data')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        response = s3_client.get_object(Bucket=os.environ['BUCKET'],Key=os.environ['KEY'])
    except Exception as get_error:
        logging.exception("GetObjectError : %s", json.dumps(get_error))

    try:
        obj = response['Body'].read()
        obj = (obj.decode('utf-8'))
    except Exception as read_error:
        logging.exception("DiffSQLScriptReadError : %s", json.dumps(read_error))

    try:
        client.execute_statement(Database=os.environ['DBNAME'], DbUser=os.environ['USER'], Sql=obj, ClusterIdentifier=os.environ['CLUSTERID'])
    except Exception as diff_error:
        logging.exception("DiffQueryExecutionError : %s", json.dumps(diff_error))
