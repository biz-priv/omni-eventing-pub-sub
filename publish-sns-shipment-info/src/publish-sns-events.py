import hashlib
import hmac
import io
import json
import boto3
import os
import logging
from pandas import read_csv, merge, DataFrame

import psycopg2

s3_client = boto3.client('s3')
ddb_client = boto3.client('dynamodb')
sns_client = boto3.client('sns')


def handler(event, context):
    # RedShift - OPEN
    try:
        con = psycopg2.connect(dbname=os.environ["DBNAME"], host=os.environ["HOST"],
                               port=os.environ["PORT"], user=os.environ["USER"], password=os.environ["PASS"])
        con.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
    except Exception as e:
        logging.exception("RedshiftConnError: {}".format(e))

    try:
        # S3 - Fetching Diff file from S3
        response = s3_client.get_object(Bucket=os.environ["BUCKET"], Key=os.environ["KEY"])
    except Exception as e:
        logging.exception("S3GetObjectError: {}".format(e))

    try:
        # Utilizing Pandas Library and reading data into a dataframe
        df = read_csv(io.BytesIO(response['Body'].read()))
        raw_data = df.dropna(subset=['bill_to_nbr'])
        raw_data = raw_data.fillna(value='NA')
        raw_data = raw_data.astype({'bill_to_nbr': 'int32'})
    except Exception as e:
        logging.exception("DataFrameCreateError: {}".format(e))

    # Generating Old data DataFrame on record_type Flag OLD
    old_data = raw_data.loc[raw_data['record_type'] == 'OLD']

    # Fetching file numbers from OLD records to filter first time entries
    old_file_nbrs = old_data.file_nbr.unique()
    old_file_nbrs = list(old_file_nbrs)

    # Generating changed_data DataFrame on record_type Flag NEW
    changed_data = raw_data.loc[raw_data['record_type'] == 'NEW']

    # Filtering the first time entries in changed_data
    changed_data = changed_data.loc[changed_data['file_nbr'].isin(old_file_nbrs)]

    # Generating tuple of bill_to_numbers and type casting them to int
    bill_to_numbers = raw_data.bill_to_nbr.unique()
    bill_to_numbers = [int(i) for i in bill_to_numbers]
    bill_to_numbers = tuple(bill_to_numbers)

    # Setting File_nbr as index and sorting the dataFrames by Index
    old_data = old_data.set_index(['file_nbr'])
    changed_data = changed_data.set_index(['file_nbr'])
    old_data = old_data.sort_index()
    changed_data = changed_data.sort_index()

    # Generating diff dataFrame by compare function
    diff = old_data.compare(changed_data)
    diff.columns.set_levels(['old', 'new'], level=1, inplace=True)
    try:
        # Fetching Customer ID, Bill to Numbers and the Source System from RedShift
        cur.execute(f"select id,cust_nbr,source_system from public.api_token where cust_nbr in {bill_to_numbers}")
        con.commit()
        x = cur.fetchall()
    except Exception as e:
        logging.exception("RedshiftExecuteQueryError: {}".format(e))

    cust_id_df = DataFrame.from_records(x, columns=['customer_id', 'bill_to_nbr', 'source_system'])
    cust_id_df = cust_id_df.astype({'bill_to_nbr': 'int32'})
    cur.close()
    con.close()

    raw_data_with_cid = merge(raw_data, cust_id_df, how='inner', left_on=['bill_to_nbr', 'source_system'],
                              right_on=['bill_to_nbr', 'source_system'])
    raw_data_with_cid = raw_data_with_cid[['bill_to_nbr', 'file_nbr', 'customer_id']]
    raw_data_with_cid = raw_data_with_cid.drop_duplicates(subset=['file_nbr'])
    raw_data_with_cid = raw_data_with_cid.set_index(['file_nbr'])
    raw_data_with_cid = raw_data_with_cid.sort_index()
    final_diff = merge(raw_data_with_cid, diff, how='inner', on='file_nbr')
    final_diff = final_diff.reset_index()
    # Dropping NaN and generating json payload
    c_id_list = tuple(final_diff.customer_id.unique())
    # print(c_id_list)

    diff_json_final = final_diff.apply(lambda x: [x.dropna()], axis=1).to_json()
    json_obj = json.loads(diff_json_final)
    # print(json_obj)

    for key in json_obj:
        clean_json = []
        shared_secret = get_shared_secret(json_obj[key][0]['customer_id'])
        shared_secret = bytes(shared_secret, 'utf-8')
        json_body = json.dumps(json_obj[key][0])
        json_body = json_body.encode('utf-8')
        signature = hmac.new(shared_secret, json_body, hashlib.sha256).hexdigest()
        # print("signature = {0}".format(signature))
        json_obj[key][0]['Signature'] = signature
        clean_json.append(json_obj[key][0])
        sns_message = json.dumps(clean_json)
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing',
                           Message=sns_message)

def get_shared_secret(cust_id):

    try:
        cust_id = str(cust_id)
        ddb_res = ddb_client.get_item(TableName=os.environ["DDBTABLE"], Key={'Customer_Id': {'S': cust_id},
                                                               'Event_Type': {'S': 'ShipmentDueDate'}})
    except Exception as e:
        logging.exception("CustomerIDFetchError: {}".format(e))

    try:
        return (ddb_res['Item']['Shared_Secret']['S'])
    except Exception as e:
        logging.exception("SharedSecretFetchError: {}".format(e))
