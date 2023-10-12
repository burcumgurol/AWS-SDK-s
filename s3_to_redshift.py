import boto3
import json
import psycopg2

def get_latest_s3_prefix(bucket_name, base_prefix):
    s3 = boto3.client('s3')

    # Get the latest year
    year_prefixes = get_s3_folders(bucket_name, base_prefix)
    latest_year = sorted(year_prefixes)[-1]

    # Get the latest month in the latest year
    month_prefixes = get_s3_folders(bucket_name, latest_year)
    latest_month = sorted(month_prefixes)[-1]

    # Get the latest day in the latest month
    day_prefixes = get_s3_folders(bucket_name, latest_month)
    latest_day = sorted(day_prefixes)[-1]

    # Get the latest hour in the latest day
    hour_prefixes = get_s3_folders(bucket_name, latest_day)
    latest_hour = sorted(hour_prefixes)[-1]

    return latest_hour

def get_s3_folders(bucket_name, prefix):
    s3 = boto3.client('s3')
    folders = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    for page in pages:
        for prefix in page.get('CommonPrefixes', []):
            folders.append(prefix['Prefix'])

    return folders

def get_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3')
    results = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                results.append(obj['Key'])

    return results

def read_s3_file(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def send_to_redshift(data):
    conn = psycopg2.connect(
        dbname='DB_NAME',
        user='DB_USER',
        password='DB_PASSWORD',
        host='REDSHIFT_ENDPOINT',
        port='5439'
    )

    cur = conn.cursor()
    for item in data:
        cur.execute(insert_sql, (item['key1'], item['key2']))

    conn.commit()
    cur.close()
    conn.close()

def main():
    bucket_name = "i40-cloudgateway-inbox-cdl"
    base_prefix = "5201/550105010/ASSEMBLY/"

    latest_prefix = get_latest_s3_prefix(bucket_name, base_prefix)
    file_keys = get_s3_files(bucket_name, latest_prefix)
    for file_key in file_keys:
        data = read_s3_file(bucket_name, file_key)
        send_to_redshift(data)

if __name__ == "__main__":
    main()
