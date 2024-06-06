import boto3
from datetime import datetime
import pytz
import urllib.parse
import urllib3
from OpenSearch import  create_OpenSearch_client
from dotenv import load_dotenv
import os

def write_to_log(status, operation, script_name, function_name):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    load_dotenv()
    S3_aws_access_key_id=os.getenv('S3_aws_access_key_id')
    S3_aws_secret_access_key=os.getenv('S3_aws_secret_access_key')
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_aws_access_key_id,
        aws_secret_access_key=S3_aws_secret_access_key)
    bucket_name = 'bucket-deals'
    current_date_utc = datetime.utcnow()
    israel_tz = pytz.timezone('Israel')

    # Convert UTC time to Israel Standard Time (IST)
    current_datetime_ist = current_date_utc.replace(tzinfo=pytz.utc).astimezone(israel_tz)

    # Format the current date in GMT+2 timezone
    current_datetime_str = current_datetime_ist.strftime('%Y-%m-%d %H:%M:%S')
    current_date_str = current_datetime_ist.strftime('%Y-%m-%d')
    status = str(status)
    operation = str(operation)
    script_name = str(script_name)
    function_name = str(function_name)

    log_document={'datetime':current_datetime_str,
                  'date':current_date_str[0:10],
                  'status':status,
                  'operation':operation,
                  'function_name':function_name,
                  'script_name':script_name}
    
    OpenSearch_client = create_OpenSearch_client()
    response = OpenSearch_client.index(
            index='log_etl',
            body=log_document,
            id=current_datetime_str.replace(' ', 'T'),  # Use ISO format for unique ID
            refresh=True
        )

    # if response['result'] not in ['created', 'updated']:
    #    print(f"Error indexing document: {response}")
    # else: print(f'insert to index {response["_index"]}: {response["result"]}')
    
    if 1==1:
        # Decode the operation parameter (assuming it may be URL-encoded)
        decoded_operation = urllib.parse.unquote(operation, encoding='utf-8')
        log_content = f"{current_datetime_str} - {status} - {decoded_operation} - {script_name} - {function_name}\n"
        log_key = f"logs/{current_date_str}_log.txt"

        # Check if the log file exists
        try:
            existing_object = s3.get_object(Bucket=bucket_name, Key=log_key)
            existing_content = existing_object['Body'].read().decode('utf-8')
        except s3.exceptions.NoSuchKey:
            # Create the log file if it doesn't exist
            existing_content = ''

        # Append new content to existing content
        new_content = existing_content + log_content

        s3.put_object(
            Bucket=bucket_name,
            Key=log_key,
            Body=new_content.encode('utf-8'),
            ContentType='text/plain; charset=utf-8',
            ACL='private'  # Adjust ACL as needed
        )

if __name__ == "__main__":

    write_to_log('TEST- Started', 'ProcessData', 'process_script.py', 'process_data')
