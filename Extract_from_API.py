# %%
import boto3
import urllib3
import json
from write_to_log import write_to_log
from extract_from_s3_and_transform import get_source_field_from_filename
from datetime import datetime
import requests
import pytz
import pandas as pd
import shutil
import os
from dotenv import load_dotenv


def extract_list_of_robots(secret_api_key):
    
    url = "https://api.browse.ai/v2/robots"
    headers = {"Authorization": f"Bearer {secret_api_key}"}
    response = requests.get(url, headers=headers, verify=False)  

    if response.status_code == 200:
        #print("Success:", response.json())
        list_of_robots=(response.json()['robots']['items'])
        return list_of_robots
    else:
        print(f"Error {response.status_code}: {response.text}")

def get_robot_name(robot_id,secret_api_key):
    list_of_robots=extract_list_of_robots(secret_api_key)
    for robot in list_of_robots:
        if robot['id']==robot_id:
            return robot['name']

def Run_a_robot():
    pass

def Get_all_tasks_by_a_robot(robotId='5be857fd-5be2-402f-b2a9-b3fee0e91d42'):
    secret_api_key=os.getenv('secret_api_key')
    url = f"https://api.browse.ai/v2/robots/{robotId}/tasks"
    querystring = {"page":"1"}
    headers = {"Authorization": f"Bearer {secret_api_key}"}
    response = requests.request("GET", url, headers=headers, params=querystring,verify=False)
    return(response.text)

def get_capturedLists_from_task(successful_tasks):
    capturedLists_all_companies=[]
    for task in successful_tasks:
        capturedLists_all_companies.append({'originUrl':task['inputParameters']['originUrl'],'capturedList':task['capturedLists']})
    companies_andcapturedLists= capturedLists_all_companies
    all_capturedLists=dict()
    for company_list in companies_andcapturedLists:
        for company_name in company_list.keys():
            all_capturedLists[company_name]=(company_list[company_name])
    return(all_capturedLists)

def retrieve_all_robot_bulk_run(robotId,secret_api_key):
    bulk_run_list=[]
    url = f"https://api.browse.ai/v2/robots/{robotId}/bulk-runs"
    querystring = {"page":"1"}
    headers = {"Authorization": f"Bearer {secret_api_key}"}
    all_bulk_runs = (requests.request("GET", url, headers=headers, params=querystring,verify=False)).json()
    if all_bulk_runs['statusCode']==200:
        for bulk_run in all_bulk_runs['result']['items']:
                print(f'Robot name={get_robot_name(robotId,secret_api_key)} Robot id={robotId}')
                print('bulk_run[createdAt]:',bulk_run['createdAt'])
                print(f"\tBulk Ran at: {convert_utc_to_GMT2(bulk_run['createdAt'])} tasksCount: {bulk_run['tasksCount']}")
                bulk_run_list.append(bulk_run)
        print(f'\t{json.dumps(bulk_run_list,indent=4, ensure_ascii=False)}')
        return bulk_run_list

def convert_utc_to_GMT2(milliseconds_timestamp): 


    # Convert milliseconds to seconds
    seconds_timestamp = milliseconds_timestamp / 1000
    israel_tz = pytz.timezone('Israel')
    try:
        # Convert the timestamp to a datetime object
        utc_datetime = datetime.utcfromtimestamp(seconds_timestamp)
        # Set the timezone to UTC
        utc_datetime = utc_datetime.replace(tzinfo=pytz.utc)
        # Convert UTC time to Israel Standard Time (IST)
        current_datetime_ist = utc_datetime.astimezone(israel_tz)
        return current_datetime_ist.strftime('%Y-%m-%d Time %H-%M-%S')
    except (OSError, TypeError) as e:
        print(f"Error converting timestamp to datetime: {e}")
        return None
    
def delete_file_from_s3(S3_aws_access_key_id, S3_aws_secret_access_key, bucket_name, file_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_aws_access_key_id,
        aws_secret_access_key=S3_aws_secret_access_key
    )
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"File '{file_key}' successfully deleted from bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error deleting file '{file_key}' from bucket '{bucket_name}': {e}")

def decode_url_parameter(encoded_url):
    import urllib.parse    
    return urllib.parse.unquote(encoded_url, encoding='utf-8')

def delete_all_contents(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove file or link
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove directory
        except Exception as e:
            write_to_log('Error',f'Failed to delete {file_path}. Reason: {e}','Extract_from_API.py','delete_all_contents')
            raise e
    write_to_log('Info',f'all files are deleted for directory:{directory}','Extract_from_API.py','delete_all_contents')

def get_all_tasks_and_store_to_files(S3_aws_access_key_id,S3_aws_secret_access_key):

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    secret_api_key=os.getenv('secret_api_key')
 
    bucket_name = 'bucket-deals'
    folder_name = 'logs/'
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y-%m-%d')
    file_key = folder_name +formatted_date +'_log.txt'
    delete_file_from_s3(S3_aws_access_key_id, S3_aws_secret_access_key, bucket_name, file_key)
    
    list_of_robots=extract_list_of_robots(secret_api_key)
    folder_deals_ready_to_load = '/opt/airflow/deals_data/files_from_api/deals_ready_to_load'
    folder_all_tasks_of_Robot =  '/opt/airflow/deals_data/files_from_api/tasks_robots'
    
    #folder=r'C:\Naya_Course\airflow\deals_data\files_from_api\\'
    write_to_log('Info',f"Extract from API has started...",'Extract_from_API.py','get_all_tasks_and_store_to_files')

    delete_all_contents(folder_deals_ready_to_load)
    for robot in list_of_robots:
        successful_tasks_of_robot=[]
        tasks_of_a_robot=json.loads(Get_all_tasks_by_a_robot(robot['id']))
        for task in tasks_of_a_robot['result']['robotTasks']['items']:
            if task['status']=='successful':
               successful_tasks_of_robot.append(task)
               for key in task['capturedLists'].keys():
                    # we currently dont want monitoring tasks to be included in the deals files:
                    if task['capturedLists'][key][0].get("_STATUS", 'Regular Task')=='Regular Task':
                        captured_data_task=task['capturedLists'][key]
                        file_name = ''.join([f"task_id={task['id']}|",'task_finished=',convert_utc_to_GMT2(task['finishedAt']),\
                                                    print_from_substring(decode_url_parameter(task['inputParameters']['originUrl']),'&'),\
                                                        sanitize_filename(robot['name'])])
                        #storing pure deals ready to be transformed:
                        with open(f"{folder_deals_ready_to_load}/{file_name}.json",'w', encoding='utf-8') as f:
                                f.write(json.dumps(captured_data_task ,indent=4, ensure_ascii=False))
                    else: continue
        #storing one file for each robot with its tasks (deals included) :
        with open(f"{folder_all_tasks_of_Robot}/All tasks of Robot={sanitize_filename(robot['name'])}.json",'w', encoding='utf-8') as f:           
            f.write(json.dumps(successful_tasks_of_robot ,indent=4, ensure_ascii=False))
        #write_to_log('Info',f"All successful tasks of robot {get_source_field_from_filename(robot['name'])} has been extracted & stored into deal files",'Extract_from_API.py','get_all_tasks_and_store_to_files')
    write_to_log('Info',f"Extract from API has finished successfully",'Extract_from_API.py','get_all_tasks_and_store_to_files')
    return 

def print_from_substring(input_string, substring):
    index = input_string.find(substring)
    if index != -1:
        return(input_string[index+1:])
    else: return ''

def sanitize_filename(filename):
    import re
    # Replace invalid characters with an underscore
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def main():
    load_dotenv()
    S3_aws_access_key_id=os.getenv('S3_aws_access_key_id')
    S3_aws_secret_access_key=os.getenv('S3_aws_secret_access_key')
    get_all_tasks_and_store_to_files(S3_aws_access_key_id,S3_aws_secret_access_key)
if __name__=='__main__':
    main()

