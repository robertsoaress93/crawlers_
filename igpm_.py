import boto3
import csv
import requests
import sys

from awsglue.utils import getResolvedOptions
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from io import StringIO

def check_last_date_from_dynamodb():
    print('[*] Checking Dynamodb...')
    client = boto3.client('dynamodb')
    table_name = 'etl_execution_reference'

    item_get = {'data_source': {'S':'igpm'}}
    
    dynamo_response = client.get_item(TableName = table_name, Key = item_get)
    item = dynamo_response.get('Item', None)
    
    print('[*] Last processed date:', item['reference_period']['S'])
    return item['reference_period']['S']

def check_object_exists(bucket):
    key = 'IGPM/'
    client = boto3.client('s3')

    response = client.list_objects_v2(Bucket=bucket, Prefix=key)

    # If there is 'Contents' in the response, the object exists in the bucket
    if 'Contents' in response:
        return True

    return False

def get_bs_object(url):
    try:
        response = requests.get(url)
    except:
        print('[*] Unable to retrieve page: {}'.format(url))
        return None

    bs = BeautifulSoup(response.text, 'html.parser')

    return bs

def get_last_date_from_igpm(data):
    # Get the last year in the list.
    # data[0] is the header.
    data = data[1]
    for index, value in enumerate(data):
        if value == '':
            last_month = str(index - 1).zfill(2)
            last_year = data[0]
            return last_year + '-' + last_month
    
    last_month = str(len(data) - 2).zfill(2)
    last_year = data[0]
    return last_year + '-' + last_month

def get_name_file(bucket:str, date:str):
    date = date.replace('-','')
    execution_time = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = None

    if "work-area" in bucket:
        file_name = "IGPM"
    else:
        file_name = 'IGPM/IGPM_Ref{}_{}'.format(date, execution_time)
    
    return file_name

def save_to_csv(data_to_save, bucket, path, date):
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter=';')
    writer.writerows(data_to_save)
        
    file_name = get_name_file(bucket, date)

    # For local test    
    # client = boto3.client('s3', endpoint_url="http://localhost:4566")
    client = boto3.client('s3')

    client.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=f'{path}/{file_name}.csv')
    print(f'[*] Saved data in bucket: {bucket}/{path}')

def scrape_page(bs):
    data = []
    table = bs.find('div', id='result-table-collapse').table
    
    header = table.find_all('th')

    row = []
    for th in header:
        row.append(th.text.upper())

    # Add the column name 'Ano' to header
    row[0] = 'ANO'

    data.append(row)

    for tr in table.find_all('tr'):
        row = []
        for index, td in enumerate(tr.find_all('td')):
            if td.text == '-':
                row.append('')
            elif index == 0:
                row.append(td.text)
            else:
                value = td.text.replace(',', '.').replace(' ', '')
                row.append('{:.2f}'.format(float(value)))
        
        # If list is not empty, append to data
        if row:
            data.append(row)

    return data

def update_dynamodb(bucket, reference_date):
    client = boto3.client('dynamodb')
    table_name = 'etl_execution_reference'

    execution_date = datetime.now(timezone.utc)
    execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S')

    item = {
        'data_source': {'S': 'igpm'},
        'reference_period': {'S': reference_date},
        'last_execution': {'S': execution_date},
        'target_bucket': {'S': bucket}
    }

    client.put_item(TableName = table_name, Item = item)

    print('[*] Dynamo updated with:', item)

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, [
        'target_bucket',
        'source_url',
        'path'
    ])
    
    target_buckets = args['target_bucket']
    source_url = args['source_url']
    path = args['path'] if "path" in args else "ECONOMIC"
    # path = "ECONOMIC"

    buckets = target_buckets.replace(" ", "").split(",")
    
    bs = get_bs_object(source_url)

    data = scrape_page(bs)

    last_igpm_date = get_last_date_from_igpm(data)

    print(buckets)

    for target_bucket in buckets:

        if check_object_exists(target_bucket):
            last_processed_date = check_last_date_from_dynamodb()
            # last_processed_date = "202207"

            if last_processed_date != last_igpm_date:
                save_to_csv(data, target_bucket, path, last_igpm_date)
                update_dynamodb(target_bucket + '/IGPM/', last_igpm_date)
            
            else:
                print('[*] Exit without changes!\n\t[-] Last extracted date: {}\n\t[-] Last IGPM date: {}'.format(last_processed_date, last_igpm_date))
        
        else:
            save_to_csv(data, target_bucket, path, last_igpm_date)
            update_dynamodb(target_bucket + '/IGPM/', last_igpm_date)
    
    