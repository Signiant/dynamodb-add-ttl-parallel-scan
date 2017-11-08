import threading
from threading import current_thread
import boto3
import argparse
import sys
import json
import decimal
import time
import datetime
import dateutil
import calendar


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def log(message):
    print current_thread().name + ": " + message


def dynamodb_connect(region):
    session = boto3.session.Session(region_name=region)
    dynamodb_client = session.client('dynamodb', region_name=region)

    return dynamodb_client


# Return the base value in seconds
# Handles 8601 string, milliseconds or seconds
def get_base_value_epoch_seconds(base_value):
    epoch_seconds = None
    base_value_float = None

    # test for ISO string first
    try:
        parsed = dateutil.parser.parse(base_value)
        log(base_value + " looks to be a time string")
        epoch_seconds = long(parsed.strftime('%s'))
    except Exception as e:
        log(base_value + " is not an ISO8601 string")

    if not epoch_seconds:
        # Could be seconds or milliseconds
        try:
            base_value_float = float(base_value)
        except Exception as e:
            log("ERROR: Unable to convert base_value to a float: " + str(e))

        if base_value_float:
            # https://stackoverflow.com/questions/23929145/how-to-test-if-a-given-time-stamp-is-in-seconds-or-milliseconds
            now = time.mktime(time.gmtime())
            if base_value_float > now:
                log(base_value + " looks to be in milliseconds - converting to seconds")
                epoch_seconds = long(base_value_float / 1000.0)
            else:
                # Could be seconds - see if we can parse it
                try:
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(base_value_float))
                    log(base_value + " looks to be in seconds already")
                    epoch_seconds = long(base_value)
                except Exception as e:
                    log(base_value + " does not look to be seconds " + str(e))

    return epoch_seconds


def get_expiry(base_value, ttl_duration):
    expiry_ttl = None

    future = datetime.datetime.fromtimestamp(float(base_value)) + datetime.timedelta(days=long(ttl_duration))
    expiry_ttl = calendar.timegm(future.timetuple())

    return expiry_ttl


def get_table_key_schema(dynamodb_client, table_name):
    key_schema = {}

    response = dynamodb_client.describe_table(TableName=table_name)

    if 'ResponseMetadata' in response:
        if 'HTTPStatusCode' in response['ResponseMetadata']:
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                log("ERROR: describing table " + table_name)
            else:
                log("Successfully described table properties for: " + table_name)
                # This is bonkers
                for key in response['Table']['KeySchema']:
                    key_name = key['AttributeName']
                    key_type = None

                    for attribute_def in response['Table']['AttributeDefinitions']:
                        if key_name == attribute_def['AttributeName']:
                            key_type = attribute_def['AttributeType']

                    key_schema[key_name] = {key_type: None}
        else:
            log("ERROR: No http status code in response when trying to describe table " + table_name)
    else:
        log("ERROR: No response metadata when trying to describe " + table_name)

    return key_schema


def update_item(table_name, key, ttl_attrib_name, ttl_attrib_value, client):
    status = False
    key_str = json.dumps(key)

    log("updating item " + key_str + " in table " + table_name)

    response = client.update_item(
        TableName=table_name,
        Key=key,
        ExpressionAttributeNames={
            '#ED': ttl_attrib_name
        },
        ExpressionAttributeValues={
            ':ed': {
                'N': str(ttl_attrib_value),
            }
        },
        ReturnValues='ALL_NEW',
        UpdateExpression='SET #ED = :ed'
    )

    if 'ResponseMetadata' in response:
        if 'HTTPStatusCode' in response['ResponseMetadata']:
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                log("ERROR: error updating key " + key_str)
                status = False
            else:
                status = True
        else:
            log("ERROR: No http status code in response when trying to update " + key_str)
            status = False
    else:
        log("ERROR: No response metadata when trying to update " + key_str)
        status = False

    return status


def set_key(key_schema, item):
    completed_key = {}
    for attrib in key_schema:
        completed_key[attrib] = item[attrib]

    return completed_key


def compute_ttl_value(master_attribute_name, item, ttl_duration):
    ttl_value = None

    if master_attribute_name in item:
        # is attribute a string or a number?
        if 'S' in item[master_attribute_name]:
            master_attribute_value = item[master_attribute_name]['S']
        elif 'N' in item[master_attribute_name]:
            master_attribute_value = item[master_attribute_name]['N']
        else:
            log("ERROR: Unknown attribute type for the master attribute")

        if master_attribute_value:
            master_epoch_seconds = get_base_value_epoch_seconds(master_attribute_value)

            if master_epoch_seconds:
                ttl_value = get_expiry(master_epoch_seconds, ttl_duration)
    else:
        log("No attribute named " + master_attribute_name + " present")

    return ttl_value


def process_segment(dynamodb_client=None, table_name=None, key_schema=None, master_attribute_name=None, ttl_duration=0, ttl_attrib_name=None, segment=0, total_segments=10):
    # This method/function is executed in each thread, each getting its
    # own segment to process through.
    log("Processing segment " + str(segment))

    # This will give us all entries without an expiry set
    filter_expression = "attribute_not_exists(#ttl)"
    attribuite_names = {"#ttl": "ttl"}

    response = dynamodb_client.scan(
        TableName=table_name,
        Segment=segment,
        TotalSegments=total_segments,
        FilterExpression=filter_expression,
        ExpressionAttributeNames=attribuite_names
    )

    for item in response['Items']:
        # print(json.dumps(item, cls=DecimalEncoder))
        current_key = set_key(key_schema, item)

        log("Read key " + json.dumps(current_key, cls=DecimalEncoder))
        ttl_value = compute_ttl_value(master_attribute_name, item, ttl_duration)

        if ttl_value:
            log("The TTL value is " + str(ttl_value) + " for key " + json.dumps(current_key, cls=DecimalEncoder))

            if update_item(table_name, current_key, ttl_attrib_name, ttl_value, dynamodb_client):
                log("Successfully updated item " + json.dumps(current_key, cls=DecimalEncoder))
            else:
                log("Error updating item " + str(ttl_value))
        else:
            log("Unable to determine a ttl value for " + json.dumps(current_key, cls=DecimalEncoder))

    # This is if we have paginated results...
    while 'LastEvaluatedKey' in response:
        response = dynamodb_client.scan(
            TableName=table_name,
            Segment=segment,
            TotalSegments=total_segments,
            FilterExpression=filter_expression,
            ExpressionAttributeNames=attribuite_names,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        for item in response['Items']:
            # print(json.dumps(item, cls=DecimalEncoder))
            current_key = set_key(key_schema, item)

            log("Read key " + json.dumps(current_key, cls=DecimalEncoder))
            ttl_value = compute_ttl_value(master_attribute_name, item, ttl_duration)

            if ttl_value:
                log("The TTL value is " + str(ttl_value) + " for key " + json.dumps(current_key, cls=DecimalEncoder))

                if update_item(table_name, current_key, ttl_attrib_name, ttl_value, dynamodb_client):
                    log("Successfully updated item " + json.dumps(current_key, cls=DecimalEncoder))
                else:
                    log("Error updating item " + str(ttl_value))
            else:
                log("Unable to determine a ttl value for " + json.dumps(current_key, cls=DecimalEncoder))


# main
def main(argv):
    result = 0

    parser = argparse.ArgumentParser(description='Add TTL attribute to existing DynamoDB table data')
    parser.add_argument('-d', '--duration', help='Add the ttl with original attribute value + this duration (in days)', required=True)
    parser.add_argument('-m', '--masterattrib', help='Existing attribute to base the ttl on (must be a timestamp)', required=True)
    parser.add_argument('-n', '--ttlname', help='Name to use for the new ttl attribute', required=True)
    parser.add_argument('-r', '--region', help='AWS region', required=True)
    parser.add_argument('-s', '--segments', help='Number of segments to use when scanning the table', required=True)
    parser.add_argument('-t', '--table', help='Table name in DynamoDB', required=True)

    args = parser.parse_args()

    dynamo_handle = dynamodb_connect(args.region)

    if dynamo_handle:
        log("Connected to DynamoDB, region " + args.region)

        # Get the key schema for the table
        key_schema = get_table_key_schema(dynamo_handle, args.table)

        if key_schema:
            log("The key schema for table " + args.table + " is " + json.dumps(key_schema, cls=DecimalEncoder))
            pool = []
            # We're choosing to divide the table in 3, then...
            pool_size = int(args.segments)

            # ...spinning up a thread for each segment.
            for i in range(pool_size):
                worker = threading.Thread(
                    target=process_segment,
                    kwargs={
                        'dynamodb_client': dynamo_handle,
                        'table_name': args.table,
                        'key_schema': key_schema,
                        'master_attribute_name': args.masterattrib,
                        'ttl_duration': args.duration,
                        'ttl_attrib_name': args.ttlname,
                        'segment': i,
                        'total_segments': pool_size,
                    }
                )
                pool.append(worker)
                # We start them to let them start scanning & consuming their
                # assigned segment.
                worker.start()

            # Finally, we wait for each to finish.
            for thread in pool:
                thread.join()
        else:
            log("Unable to determine key schema for table " + args.table)
            result = 1

    exit(result)


if __name__ == "__main__":
    main(sys.argv[1:])
