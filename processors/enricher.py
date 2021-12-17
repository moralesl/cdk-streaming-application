import base64
import json
import boto3
import os

DESTINATION_STREAM = os.environ['DESTINATION_STREAM']
USER_INFO_TABLE = os.environ['USER_INFO_TABLE']

kinesis_client = boto3.client('kinesis')
dynamodb_client = boto3.client("dynamodb")

print('Loading function')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    print("Received events: %d" % len(event['Records']))
    
    enriched_orders_list = enrich_incoming_orders(event['Records'])
    
    # Let's add the matched records to the output kinesis stream
    response = put_records_to_stream(enriched_orders_list)

    # If there was some failure, go ahead and print out the details
    if response['FailedRecordCount'] > 0:
        print('FailedRecordCount = %d' % response['FailedRecordCount'])
        print('Received event: ' + json.dumps(event, indent=2))
        print('Records: ' + json.dumps(enriched_orders_list, indent=2))
        raise Exception('FailedRecordCount = %d' % response['FailedRecordCount'])
    else:
        print('Successfully put %d record(s) onto output stream.' % len(enriched_orders_list))
        print(*enriched_orders_list, sep = ", ")    


def enrich_incoming_orders(records):
    user_id_set = set()
    incoming_orders_list = []
    for record in records:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        incoming_order = json.loads(payload)
        user_id_set.add(incoming_order['user_id'])
        incoming_orders_list.append(incoming_order)

    user_id_dict = get_matching_user_info_records(user_id_set)

    enriched_orders_list = []
    for incoming_order in incoming_orders_list:
        enriched_record = incoming_order.copy()
        if incoming_order['user_id'] in user_id_dict:
            user_id = incoming_order['user_id']
            user = user_id_dict[user_id]

            enriched_record['first_name'] = user['first_name']['S']
            enriched_record['last_name'] = user['last_name']['S']
            enriched_record['email'] = user['email']['S']

        enriched_orders_list.append(enriched_record)
    
    return enriched_orders_list


def get_matching_user_info_records(user_id_set):
    user_id_dict = {}

    # Convert the set of user_ids into a format for the batch_get_item API
    record_keys = map(lambda i: {'user_id':{'S':i}}, user_id_set)

    # Retrieve the records in batches using batch_get_item
    response = dynamodb_client.batch_get_item(
        RequestItems = {
            USER_INFO_TABLE: {
                'Keys': list(record_keys),
                'AttributesToGet': [
                    'user_id','first_name','last_name','email'
                ],
                'ConsistentRead': True,
            }
        },
        ReturnConsumedCapacity='TOTAL'
    )
    
    # Loop through all of ther records returned from DynamoDB
    # and populate the user_id_dict with matching user_ids
    for i in response['Responses'][USER_INFO_TABLE]:
        user_id_dict[i['user_id']['S']] = i
        
    return user_id_dict

def put_records_to_stream(orders_list = []):
    if len(orders_list) > 0:
        kinesis_client = boto3.client('kinesis')
        response = kinesis_client.put_records(
            StreamName = DESTINATION_STREAM,
            Records = list(map(lambda record: {
                            'Data': json.dumps(record),
                            'PartitionKey':record['user_id']
                        },
                        orders_list))
        )
        return response
    else:
        return {'FailedRecordCount':0}
