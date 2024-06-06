import boto3
import json
from write_to_log import write_to_log
import os 
import urllib3
from dotenv import load_dotenv



def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    load_dotenv()
    aws_access_key_id=os.getenv('aws_access_key_id')
    aws_secret_access_key=os.getenv('aws_secret_access_key')
    # Define your bucket name and local folder
    bucket_name = 'bucket-deals'
    #local_folder_path = r'C:\Naya_Course\airflow\deals_data\files_from_api\deals_ready_to_load'
    local_folder_path=r'/opt/airflow/deals_data/files_from_api/deals_ready_to_load'
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    try:
        file_list = [f for f in os.listdir(local_folder_path) if f.endswith('.json')]
        if not file_list:
            write_to_log('Info', f'No files found in local folder {local_folder_path}', 'store_into_s3bucket.py', 'main')
            return
        #write_to_log('Info', f'The files in local folder {local_folder_path}:\n{chr(9)}{(chr(10)+chr(9)).join(file_list)}', 'store_into_s3bucket.py', 'main')
    except Exception as e:
        write_to_log('Error', f'Failed to list files in local folder {local_folder_path}: {str(e)}', 'store_into_s3bucket.py', 'main')
        return
    try:
        # Upload each JSON file to a different S3 location
        counter = 0
        for file_name in file_list:
            counter += 1
            file_path = os.path.join(local_folder_path, file_name)

            # Read the JSON file from local folder
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            new_s3_key = f'json_deals/{file_name}'
            # Upload the JSON data to S3
            s3.put_object(
                Bucket=bucket_name,
                Key=new_s3_key,
                Body=json.dumps(json_data),
                ContentType='application/json'
            )

            print(f"Uploaded {file_name} to S3 bucket {bucket_name} with key {new_s3_key}")
        write_to_log('Info', f'{counter} JSON files processed from local folder {local_folder_path}', 'store_into_s3bucket.py', 'main')
    
    except Exception as e:
        write_to_log('Error', f'Failed processing JSON files from local folder {local_folder_path}: {str(e)}', 'store_into_s3bucket.py', 'main')
        print('Raised ERROR! Failed processing JSON files from local folder')

if __name__ == '__main__':
    main()
