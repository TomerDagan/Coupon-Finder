from dotenv import load_dotenv
from write_to_log import write_to_log
import json
import boto3
import re
import datetime
import os


def find_values_in_string(input_string):    
    try:
        values_to_search = ["ראשון", "שני", "שלישי", "רביעי", "חמישי", "שישי", "שבת"]
        found_values = []
        # Iterate over each value and check if it's in the input string
        for value in values_to_search:
            if value in input_string:
                found_values.append(value)
        return found_values
    except Exception as e:
        write_to_log('Error:',f"{input_string}': {e} ",'extract_from_s3_and_transform.py','find_values_in_string')
        raise e

def is_date_expired(expire_date):
    # Extract the date from the "expire_date" string
    date_str = expire_date.split()[-1]  # Assuming the date is always at the end of the string
    # Parse the extracted date string
    try:
        expire_date = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
    except ValueError:
        # If the date string cannot be parsed, return False (not expired)
        return False
    # Get the current date
    current_date = datetime.date.today()
    
    # Compare the expire_date to the current_date
    return expire_date <= current_date

def find_first_and_second_numbers(input_string_price, input_string_deal):
    try:
        pattern = r'(\d+|\d{1,3}(,\d{3})*)(\.\d+)?'
        numbers_in_price = re.findall(pattern, input_string_price)
        numbers_in_deal = re.findall(pattern, input_string_deal)    
        # Extract the first and second numbers
        if len(numbers_in_price) >= 2:
            first_number = int(numbers_in_price[0][0].replace(',', ''))  
            second_number = int(numbers_in_price[1][0].replace(',', ''))  
            return first_number, second_number
        elif len(numbers_in_price) == 1 and len(numbers_in_deal) >= 1:
            first_number = int(numbers_in_price[0][0].replace(',', ''))  
            second_number = int(numbers_in_deal[0][0].replace(',', ''))  
            return first_number, second_number
        elif len(numbers_in_price) >= 1 and len(numbers_in_deal) == 1:
            first_number = int(numbers_in_price[0][0].replace(',', ''))  
            second_number = int(numbers_in_deal[0][0].replace(',', ''))  
            return first_number, second_number
        else:    
            return None, None
    except Exception as e:
        write_to_log('Error:',f"deal {input_string_deal}': {e} ",'extract_from_s3_and_transform.py','find_first_and_second_numbers')
        raise e

def extract_specific_keys(original_dict, keys_to_extract):
    return {key: original_dict[key] for key in keys_to_extract if key in original_dict}

def return_transformed_deals(S3_aws_access_key_id, S3_aws_secret_access_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_aws_access_key_id,
        aws_secret_access_key=S3_aws_secret_access_key
    )
    bucket_name = 'bucket-deals'
    folder_name = 'json_deals/'  # Assuming your JSON files are stored in this folder in the bucket

    all_deals = []

    try:
        # List objects in the specified folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' in response:
            # Loop through each object in the folder
            for obj in response['Contents']:
                # Get the object key (file name)
                key = obj['Key']
                print("Processing:", key)

                # Download the JSON file from S3
                try:
                    response = s3.get_object(Bucket=bucket_name, Key=key)
                    # Read the JSON data
                    json_data = response['Body'].read().decode('utf-8')
                    deals_list = json.loads(json_data)

                    # Process the JSON data
                    source_field = get_source_field_from_filename(key)  # Determine source field based on file name
                    for deal in deals_list:
                        try:
                            # Modify the deal data as needed
                            deal['source'] = source_field
                            deal['price'] = deal.get('price', '') or ''  # Ensure 'price' is not None
                            deal['deal'] = deal.get('deal', '') or ''  # Ensure 'deal' is not None

                            # Ensure 'price' and 'deal' are strings
                            if not isinstance(deal['price'], str):
                                deal['price'] = str(deal['price'])
                            if not isinstance(deal['deal'], str):
                                deal['deal'] = str(deal['deal'])

                            deal['is_starting_from'] = True if 'החל מ-\n' in deal['price'] else False
                            deal['deal'] = deal['deal'].replace('\\n', '')
                            deal['price'] = deal['price'].replace('\n', '') if deal['price'] else ''
                            #handle the situation where there is number without '₪':
                            if '₪' not in deal['price'] and len(deal['price'])>1:
                                deal['price']= '₪' + deal['price']
                            #handle the situation where '₪' is the first char:
                            #elif '₪'==deal['price'][len(deal['price'])-1]:
                            #    deal['price']='₪'+deal['price'][0:-1]
                            #handle the situation where there is duplication in description:
                            if deal['price']==deal['deal']:
                                deal['price']=''
                            deal['deal_for_day'] = find_values_in_string(deal['deal'])
                            deal['deal_id'] = f"{source_field}|{deal['deal']}"
                            modified_date = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                            first_num, second_num = find_first_and_second_numbers(deal['price'], deal['deal'])
                            if first_num is not None and second_num is not None:
                                deal['discount_in_money'] = float(second_num - first_num)
                                deal['discount_in_percents'] = f"{((1 - (first_num / second_num)) * 100):.1f}%"
                            deal['extraction_date'] = modified_date
                            if 'expire_date' in deal and deal['expire_date'] != 0:
                                deal['is_expired'] = is_date_expired(deal['expire_date'])

                            # Add the modified deal data to the list
                            all_deals.extend(deals_list)

                        except Exception as e:
                            keys_to_extract = ['Position', 'price', 'deal', 'source']
                            filtered_deal_for_log = extract_specific_keys(deal, keys_to_extract)
                            write_to_log('Error:', f"processing deal {deal}| source:'{source_field}': {e}", 'extract_from_s3_and_transform.py', 'return_transformed_deals')
                            print(f"Error processing deal {filtered_deal_for_log}| source:'{source_field}': {e}")
                            continue

                except Exception as e:
                    write_to_log('Error:', f'Error processing file {key}: {e}', 'extract_from_s3_and_transform.py', 'return_transformed_deals')
                    print(f"Error processing file {key}: {e}")
                    continue

    except Exception as e:
        write_to_log('Error:', f'Error listing objects in S3 bucket: {e}', 'extract_from_s3_and_transform.py', 'return_transformed_deals')
        print(f"Error listing objects in S3 bucket: {e}")

    write_to_log('Info:', f'{len(all_deals)} deals processed', 'extract_from_s3_and_transform.py', 'return_transformed_deals')
    print('num of deals: ',len(all_deals))
    #print(all_deals)
    directory = '/opt/airflow/deals_data/files_from_api/deals_ready_to_load_to_open_search/'
    with open(f'{directory}all_deals.json','w', encoding='utf-8') as f:
        f.write(json.dumps(all_deals,indent=3, ensure_ascii=False))
    return all_deals

def get_source_field_from_filename(filename):
    if 'שופרסל' in filename:
        return 'מועדון שופרסל'
    elif 'הפיס' in filename:
        return 'מועדון מפעל הפיס'
    elif 'Cal' in filename:
        return 'כאל'
    elif 'max' in filename:
        return 'מקס'
    elif 'htzone' in filename:
        return 'הייטקזון'
    elif 'מועדון הוט' in filename:
        return 'מועדון אשראי הוט'
    elif 'ישראכרט'  in filename:
        return 'ישראכרט'
    elif 'שלך'  in filename:
        return 'מועדון שלך'
    else:
        return ''

def move_processed_files_to_archive_folder(S3_aws_access_key_id,S3_aws_secret_access_key):
    from write_to_log import write_to_log
    import boto3
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_aws_access_key_id,
        aws_secret_access_key=S3_aws_secret_access_key
    )

    # Bucket and folder names
    source_bucket = 'bucket-deals'
    source_folder = 'json_deals/'
    destination_folder = 'json_deals_archive/'

    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)

    num_of_file=0
    # Move each object to the destination folder
    for obj in response.get('Contents', []):
        num_of_file+=1
        source_key = obj['Key']
        destination_key = source_key.replace(source_folder, destination_folder, 1)
        s3.copy_object(
            Bucket=source_bucket,
            Key=destination_key,
            CopySource={'Bucket': source_bucket, 'Key': source_key}
        )
        # Delete the original object after successful copy
        print(f'moved to archive- Bucket: {source_bucket}, Key: {source_key}')
        s3.delete_object(Bucket=source_bucket, Key=source_key)
    write_to_log('Info:',f'{num_of_file} json files moved to archive','extract_from_s3_and_transform.py','return_transformed_deals')

    # Remove objects from the source folder (optional)
    for obj in response.get('Contents', []):
        s3.delete_object(Bucket=source_bucket, Key=obj['Key'])

def main():
    load_dotenv()
    S3_aws_access_key_id=os.getenv('S3_aws_access_key_id')
    S3_aws_secret_access_key=os.getenv('S3_aws_secret_access_key')
    all_deals=(return_transformed_deals(S3_aws_access_key_id,S3_aws_secret_access_key))
    move_processed_files_to_archive_folder(S3_aws_access_key_id,S3_aws_secret_access_key)
    return all_deals

# %%
if __name__=='__main__':
    pass
    main()

