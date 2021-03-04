import hashlib
import hmac
import io
import json
import boto3
import os
import logging
from pandas import read_csv, merge, DataFrame
import psycopg2

ddb_client = boto3.client('dynamodb')
sns_client = boto3.client('sns')

event_map = {"shipment-info-change": "arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing",
             "shipment-info-full": "arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing"}


def handler(event, context):
    if(event['existing'] != 'true'):
    dataframe = get_s3_object(event['s3Bucket'], event['s3Key'])
    json_obj, json_obj_full = get_events_json(dataframe)
    # print(json_obj)
    # print("--------------------****************************************---------------------------------------------")
    # print(json_obj_full)
    if ("shipment-info" in event['s3Key']):
        change_topic_arn = event_map["shipment-info-change"]
        full_topic_arn = event_map["shipment-info-full"]

    json_diff_list = publish_message(json_obj, 'change', change_topic_arn)
    json_full_list = publish_message(json_obj_full, 'full', full_topic_arn)
    merged_json_publish_list = json_full_list + json_diff_list
    # print(len(merged_json_publish_list))
    # print(len(json_full_list))
    # print(len(json_diff_list))
    count = 0
    while count<3:
        message = merged_json_publish_list[count]
        #print(message["SNS_FLAG"])
        sns_publish(message)
        count = count + 1


def get_events_json(dataframe):
    try:
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
        # print(json_obj_full)
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
        ddb_res = ddb_client.get_item(TableName=os.environ["DDBTABLE"], Key={'Customer_Id': {'S': cust_id},
                                                                             'Event_Type': {'S': 'ShipmentDueDate'}})
    except Exception as e:
        logging.exception("DynamoDBCQueryExecutionError: {}".format(e))
    try:
        if 'Item' in ddb_res:
            return ddb_res['Item']['Shared_Secret']['S']
        else:
            return None
    except Exception as e:
        logging.exception("SharedSecretFetchError: {}".format(e))


def get_cust_id(bill_to_numbers):
    con = psycopg2.connect(dbname='test_datamodel',
                           host='omni-dw-prod.cnimhrgrtodg.us-east-1.redshift.amazonaws.com',
                           port='5439', user='bceuser1', password='BizCloudExp1')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute(f"select id,cust_nbr,source_system from public.api_token where cust_nbr in {bill_to_numbers}")
    con.commit()
    x = cur.fetchall()
    cust_id_df = DataFrame.from_records(x, columns=['customer_id', 'bill_to_nbr', 'source_system'])
    cust_id_df = cust_id_df.astype({'bill_to_nbr': 'int32'})
    cur.close()
    con.close()
    return x


def sns_publish(message):
    if message["SNS_FLAG"]=="DIFF":
        topicArn = "arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing"
    elif message["SNS_FLAG"]=="FULL":
        topicArn = "arn:aws:sns:us-east-1:332281781429:test_topic_bizcloud_sns_eventing"
    customer_id = str(message['customer_id'])
    message = json.dumps(message)
    print(message)
    sns_client.publish(TopicArn=topicArn,
                       Message=message,
                       MessageAttributes={
                        'customer_id': {
                            'DataType': 'String',
                            'StringValue': customer_id
                        }})


def publish_message(payload, payloadType, topicArn):
    json_list = []
    try:
        for record in payload:
            if payloadType == 'full':
                customer_id = record['customer_id']
            elif payloadType == 'change':
                customer_id = payload[record][0]['customer_id']
                record = payload[record][0]
            shared_secret = get_shared_secret(customer_id)
            if shared_secret is not None:
                shared_secret = bytes(shared_secret, 'utf-8')
                json_body = json.dumps(record).encode('utf-8')
                record['Signature'] = hmac.new(shared_secret, json_body, hashlib.sha256).hexdigest()
                json_list.append(record)
                # message = json.dumps(record)
        return json_list


    except Exception as e:
        logging.exception("PublishMessageError: {}".format(e))
        raise SharedSecretFetchError


def merge_rawdata_with_customer_id(raw_data, cust_id_df):
    raw_data_with_cid = merge(raw_data, cust_id_df, how='inner', left_on=['bill_to_nbr', 'source_system'],
                              right_on=['bill_to_nbr', 'source_system'])
    raw_data_with_cid = raw_data_with_cid[['bill_to_nbr', 'file_nbr', 'customer_id']]
    raw_data_with_cid = raw_data_with_cid.drop_duplicates(subset=['file_nbr'])
    raw_data_with_cid = raw_data_with_cid.set_index(['file_nbr'])
    raw_data_with_cid = raw_data_with_cid.sort_index()
    return raw_data_with_cid


class SharedSecretFetchError(Exception): pass
