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
    try:
        con = psycopg2.connect(dbname=os.environ["DBNAME"], host=os.environ["HOST"],
                               port=os.environ["PORT"], user=os.environ["USER"], password=os.environ["PASS"])
        con.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
    except Exception as e:
        logging.exception("RedshiftConnError: {}".format(e))

        try:
        response = s3_client.get_object(Bucket=os.environ["BUCKET"], Key=os.environ["KEY"])
    except Exception as e:
        logging.exception("S3GetObjectError: {}".format(e))

    try:
        df = read_csv(io.BytesIO(response['Body'].read()))
        raw_data = df.dropna(subset=['bill_to_nbr'])
        raw_data = raw_data.fillna(value='NA')
        raw_data = raw_data.astype({'bill_to_nbr': 'int32'})
    except Exception as e:
        logging.exception("DataFrameCreateError: {}".format(e))

    try:
        old_data = raw_data.loc[raw_data['record_type'] == 'OLD']
        old_file_nbrs = old_data.file_nbr.unique()
        old_file_nbrs = list(old_file_nbrs)
        changed_data = raw_data.loc[raw_data['record_type'] == 'NEW']
        new_data = changed_data
        changed_data = changed_data.loc[changed_data['file_nbr'].isin(old_file_nbrs)]
        bill_to_numbers = raw_data.bill_to_nbr.unique()
        bill_to_numbers = [int(i) for i in bill_to_numbers]
        bill_to_numbers = tuple(bill_to_numbers)
        old_data = old_data.set_index(['file_nbr'])
        changed_data = changed_data.set_index(['file_nbr'])
        new_data = new_data.set_index(['file_nbr'])
        old_data = old_data.sort_index()
        changed_data = changed_data.sort_index()
        new_data = new_data.sort_index()
        diff = old_data.compare(changed_data)
        diff.columns.set_levels(['old', 'new'], level=1, inplace=True)
    except Exception as e:
        logging.exception("DataProcessingError: {}".format(e))

    try:
        cur.execute(f"select id,cust_nbr,source_system from public.api_token where cust_nbr in {bill_to_numbers}")
        con.commit()
        x = cur.fetchall()
        cust_id_df = DataFrame.from_records(x, columns=['customer_id', 'bill_to_nbr', 'source_system'])
        cust_id_df = cust_id_df.astype({'bill_to_nbr': 'int32'})
        cur.close()
        con.close()
    except Exception as e:
        logging.exception("RedshiftQueryExecutionError: {}".format(e))

    try:
        raw_data_with_cid = merge(raw_data, cust_id_df, how='inner', left_on=['bill_to_nbr', 'source_system'],
                                  right_on=['bill_to_nbr', 'source_system'])
        raw_data_with_cid = raw_data_with_cid[['bill_to_nbr', 'file_nbr', 'customer_id']]
        raw_data_with_cid = raw_data_with_cid.drop_duplicates(subset=['file_nbr'])
        raw_data_with_cid = raw_data_with_cid.set_index(['file_nbr'])
        raw_data_with_cid = raw_data_with_cid.sort_index()
        final_diff = merge(raw_data_with_cid, diff, how='inner', on='file_nbr')
        final_diff = final_diff.reset_index()
        full_payload = merge(raw_data_with_cid, new_data, how='inner', on='file_nbr')
        full_payload = full_payload.reset_index()
        # Dropping NaN and generating json payload
        diff_json_final = final_diff.apply(lambda x: [x.dropna()], axis=1).to_json()
        json_obj = json.loads(diff_json_final)
        full_json_final = full_payload.to_json(orient='records')
        json_obj_full = json.loads(full_json_final)
    except Exception as e:
        logging.exception("JSONDataProcessingError: {}".format(e))


    for key in json_obj_full:
        shared_secret = get_shared_secret(key['customer_id'])
        shared_secret = bytes(shared_secret, 'utf-8')
        json_body = json.dumps(key)
        json_body = json_body.encode('utf-8')
        signature = hmac.new(shared_secret, json_body, hashlib.sha256).hexdigest()
        key['Signature'] = signature
        sns_message = json.dumps(key)
        print(sns_message)
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing',
                           Message=sns_message)

    for key in json_obj:
        shared_secret = get_shared_secret(json_obj[key][0]['customer_id'])
        shared_secret = bytes(shared_secret, 'utf-8')
        json_body = json.dumps(json_obj[key][0])
        json_body = json_body.encode('utf-8')
        signature = hmac.new(shared_secret, json_body, hashlib.sha256).hexdigest()
        json_obj[key][0]['Signature'] = signature
        sns_message = json.dumps(json_obj[key][0])
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing',
                           Message=sns_message)


def get_shared_secret(cust_id):
    try:
        cust_id = str(cust_id)
        ddb_res = ddb_client.get_item(TableName=os.environ["DDBTABLE"], Key={'Customer_Id': {'S': cust_id},
                                                                             'Event_Type': {'S': 'ShipmentDueDate'}})
        return (ddb_res['Item']['Shared_Secret']['S'])
    except Exception as e:
        logging.exception("SharedSecretFetchError: {}".format(e))
