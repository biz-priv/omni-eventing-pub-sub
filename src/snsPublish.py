import hashlib
import hmac
import io
import json
import boto3
import os
import logging
from pandas import read_csv, merge, DataFrame
import psycopg2
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


sns_client = boto3.client('sns')
event_map = {"shipment-info-change": os.environ["SHIPMENT_INFO_SNS_CHANGE_ARN"],
             "shipment-info-full": os.environ["SHIPMENT_INFO_SNS_FULL_ARN"],
             "milestone": "",
             "milestone-full": "",
             "invoice-change": "SNS_TEST_ARN",
             "invoice-full": "SNS_TEST_ARN"
             }


def handler(event, context):
    if('existing' not in event):
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        dataframe = get_s3_object(bucket, key)
        if('dev-diff-shipment-info/dev-shipment-info-diff.csv000' in key):
            diff_payload, full_payload = get_events_json_shipments(dataframe)
        elif('dev-diff-shipment-info/dev-customer-invoices-diff.csv000' in key):
            diff_payload, full_payload = get_events_json_invoices(dataframe)
        print("diff_payload")
        print(diff_payload)
        print("full_payload")
        print(full_payload)
        diff_list = include_shared_secret(diff_payload, 'change')
        full_list = include_shared_secret(full_payload, 'full')
        merged_list = full_list + diff_list
        print("########MERGED LIST##########")
        print(merged_list)
    else:
        merged_list = event['input']

    if(len(merged_list) == 0):
        event['end'] = 'true'
    else:
        count = 0
        for message in merged_list:
            index = merged_list.index(message)
            sns_publish(message, 'shipment-info')
            merged_list[index]['published'] = 'true'
            count += 1
            if (count >= 20):
                break

        if(len([d for d in merged_list if 'published' not in d]) == 0):
            event['end'] = 'true'
        else:
            event['end'] = 'false'
    event['existing'] = 'true'
    event['input'] = [d for d in merged_list if 'published' not in d]

    return event

def get_events_json_shipments(dataframe):
    try:
        print("#############SHIPMENTS-INFO############")
        raw_data = ((dataframe.dropna(subset=['bill_to_nbr'])).fillna(value='NA')).astype({'bill_to_nbr': 'int32'})
        old_data = raw_data.loc[raw_data['record_type'] == 'OLD']
        old_file_nbrs = list(old_data.file_nbr.unique())
        changed_data = raw_data.loc[raw_data['record_type'] == 'NEW']
        new_data = changed_data
        changed_data = changed_data.loc[changed_data['file_nbr'].isin(old_file_nbrs)]
        bill_to_numbers = raw_data.bill_to_nbr.unique()
        bill_to_numbers = tuple([int(i) for i in bill_to_numbers])
        old_data = old_data.set_index(['file_nbr'])
        changed_data = changed_data.set_index(['file_nbr'])
        new_data = new_data.set_index(['file_nbr'])
        old_data = old_data.sort_index()
        changed_data = changed_data.sort_index()
        new_data = new_data.sort_index()
        diff = old_data.compare(changed_data)
        diff.columns.set_levels(['old', 'new'], level=1, inplace=True)
        db_cust_ids = get_cust_id(bill_to_numbers)
        cust_id_df = DataFrame.from_records(db_cust_ids, columns=['customer_id', 'bill_to_nbr', 'source_system'])
        cust_id_df = cust_id_df.astype({'bill_to_nbr': 'int32'})
        raw_data_with_cid = merge_rawdata_with_customer_id(raw_data, cust_id_df)
        final_diff = merge(raw_data_with_cid, diff, how='inner', on='file_nbr')
        final_diff = final_diff.reset_index()
        final_diff['SNS_FLAG'] = 'DIFF'
        full_payload = merge(raw_data_with_cid, new_data, how='inner', on='file_nbr')
        full_payload = full_payload.reset_index()
        full_payload['SNS_FLAG'] = 'FULL'
        json_obj = json.loads(final_diff.apply(lambda x: [x.dropna()], axis=1).to_json())
        json_obj_full = json.loads(full_payload.to_json(orient='records'))
        return json_obj, json_obj_full
    except Exception as e:
        logging.exception("GetEventsJsonError: {}".format(e))

def get_events_json_invoices(dataframe):
    try:
        raw_data = (dataframe.dropna(subset=['bill_to_nbr'])).fillna(value='NA')
        old_data = raw_data.loc[raw_data['record_type'] == 'OLD']
        old_file_nbrs = list(old_data.file_nbr.unique())
        new_file_nbrs = list(old_data.file_nbr.unique())
        changed_data = raw_data.loc[raw_data['record_type'] == 'NEW']
        new_file_nbrs = list(changed_data.file_nbr.unique())
        new_data = changed_data
        changed_data = changed_data.loc[changed_data['file_nbr'].isin(old_file_nbrs)]
        bill_to_numbers = raw_data.bill_to_nbr.unique()
        bill_to_numbers = tuple([i for i in bill_to_numbers])
        print("bill_to_numbers")
        print(bill_to_numbers)
        old_data = old_data.set_index(['id'])
        changed_data = changed_data.set_index(['id'])
        new_data = new_data.set_index(['id'])
        old_data = old_data.sort_index()
        changed_data = changed_data.sort_index()
        new_data = new_data.sort_index()
        old_data = old_data.loc[old_data['file_nbr'].isin(new_file_nbrs)]
        diff = old_data.compare(changed_data)
        diff.columns.set_levels(['old', 'new'], level=1, inplace=True)
        db_cust_ids = get_cust_id(bill_to_numbers)
        cust_id_df = DataFrame.from_records(db_cust_ids, columns=['customer_id', 'bill_to_nbr', 'source_system'])
        raw_data_with_cid = merge_rawdata_with_customer_id(raw_data, cust_id_df)
        final_diff = merge(raw_data_with_cid, diff, how='inner', on='id')
        final_diff = final_diff.reset_index()
        final_diff['SNS_FLAG'] = 'DIFF'
        full_payload = merge(raw_data_with_cid, new_data, how='inner', on='id')
        full_payload = full_payload.reset_index()
        full_payload['SNS_FLAG'] = 'FULL'
        json_obj = json.loads(final_diff.apply(lambda x: [x.dropna()], axis=1).to_json())
        json_obj_full = json.loads(full_payload.to_json(orient='records'))
        return json_obj, json_obj_full
    except Exception as e:
        logging.exception("GetEventsJsonError: {}".format(e))

def get_s3_object(bucket, key):
    try:
        client = boto3.client('s3')
        response = client.get_object(Bucket=bucket, Key=key)
        dataframe = read_csv(io.BytesIO(response['Body'].read()))
        return dataframe
    except Exception as e:
        logging.exception("S3GetObjectError: {}".format(e))
def get_shared_secret(cust_id):
    try:
        cust_id = str(cust_id)
        client = boto3.client('dynamodb')
        response = client.get_item(TableName=os.environ["DDBTABLE"],
            Key={'Customer_Id': {'S': cust_id}, 'Event_Type': {'S': 'ShipmentUpdate'}})
        if 'Item' in response:
            return response['Item']['Shared_Secret']['S']
        else:
            return None
    except Exception as e:
        logging.exception("DynamoDBCQueryExecutionError: {}".format(e))
def get_cust_id(bill_to_numbers):
    con = psycopg2.connect(dbname=os.environ['DBNAME'],
                           host=os.environ['HOST'],
                           port=os.environ['PORT'], user=os.environ['USER'], password=os.environ['PASS'])
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute(f"select id,cust_nbr,source_system from public.api_token where cust_nbr in {bill_to_numbers}")
    con.commit()
    x = cur.fetchall()
    print("x")
    print(x)
    cust_id_df = DataFrame.from_records(x, columns=['customer_id', 'bill_to_nbr', 'source_system'])
    cur.close()
    con.close()
    return cust_id_df
def get_topic_arn(event_type):
    if ("shipment-info" in event_type):
        change_topic_arn = event_map["shipment-info-change"]
        full_topic_arn = event_map["shipment-info-full"]
    elif ("invoice" in event_type):
        change_topic_arn = event_map["invoice-change"]
        full_topic_arn = event_map["invoice-full"]
    elif ("milestone" in event_type):
        change_topic_arn = event_map["milestone-change"]
        full_topic_arn = event_map["milestone-full"]

    return change_topic_arn, full_topic_arn
def sns_publish(message, event_type):
    try:
        change_topic_arn, full_topic_arn = get_topic_arn(event_type)
        if message["SNS_FLAG"]=="DIFF":
            topic_arn = change_topic_arn
        elif message["SNS_FLAG"]=="FULL":
            topic_arn = full_topic_arn
        customer_id = str(message['customer_id'])
        message = json.dumps(message)
        sns_client.publish(TopicArn=topic_arn,
                           Message=message,
                           MessageAttributes={
                            'customer_id': {
                                'DataType': 'String',
                                'StringValue': customer_id
                            }})
    except Exception as e:
        logging.exception("SNSPublishError: {}".format(e))

def include_shared_secret(payload, payload_type):
    json_list = []
    try:
        for record in payload:
            if payload_type == 'full':
                customer_id = record['customer_id']
            elif payload_type == 'change':
                customer_id = payload[record][0]['customer_id']
                record = payload[record][0]
            shared_secret = get_shared_secret(customer_id)
            if shared_secret is not None:
                shared_secret = bytes(shared_secret, 'utf-8')
                json_body = json.dumps(record).encode('utf-8')
                record['Signature'] = hmac.new(shared_secret, json_body, hashlib.sha256).hexdigest()
                json_list.append(record)
        return json_list
    except Exception as e:
        logging.exception("PublishMessageError: {}".format(e))
        raise SharedSecretFetchError
def merge_rawdata_with_customer_id(raw_data, cust_id_df):
    raw_data_with_cid = merge(raw_data, cust_id_df, how='inner', left_on=['bill_to_nbr', 'source_system'],
                              right_on=['bill_to_nbr', 'source_system'])
    logger.info(raw_data_with_cid)
    raw_data_with_cid = raw_data_with_cid[['bill_to_nbr', 'file_nbr', 'customer_id', 'id']]
    raw_data_with_cid = raw_data_with_cid.drop_duplicates(subset=['id'])
    raw_data_with_cid = raw_data_with_cid.set_index(['id'])
    raw_data_with_cid = raw_data_with_cid.sort_index()
    logger.info(raw_data_with_cid)
    return raw_data_with_cid
class SharedSecretFetchError(Exception): pass