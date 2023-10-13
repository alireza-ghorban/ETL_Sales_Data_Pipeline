import boto3
import time
import datetime
import subprocess
from send_email import send_email
import json

def lambda_handler(event, context):
    s3_file_list = []

    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Fetch the list of objects (files) from the specified S3 bucket
    for obj in s3_client.list_objects_v2(Bucket='midterm-data-dump-ag')['Contents']:
        s3_file_list.append(obj['Key'])
    print("s3_file_list:", s3_file_list)

    # Set the timezone (Montreal Standard Time) for fetching the current time
    tz = datetime.timezone(datetime.timedelta(hours=-4))

    # Get the current time in the specified timezone
    current_time = datetime.datetime.now(tz)
    datestr = current_time.strftime("%Y%m%d")

    # Create a list of filenames that we expect to find in the S3 bucket based on the current date
    required_file_list = [
        f'calendar_{datestr}.csv',
        f'inventory_{datestr}.csv',
        f'product_{datestr}.csv',
        f'sales_{datestr}.csv',
        f'store_{datestr}.csv'
    ]
    print("required_file_list:", required_file_list)

    # Check if all required files are present in the S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        # Generate the full S3 URLs for the files
        s3_file_url = ['s3://midterm-data-dump-ag/' + a for a in s3_file_list]
        print("s3_file_url:", s3_file_url)

        # Extract the table name from the file names (removes the last 13 characters, i.e., date and file extension)
        table_name = [a[:-13] for a in s3_file_list]
        print("table_name:", table_name)

        # Create a JSON configuration with table names as keys and corresponding S3 URLs as values
        data = json.dumps({'conf': {a: b for a, b in zip(table_name, s3_file_url)}})
        print("data:", data)

        # Send the above data to an Airflow endpoint, signaling it to start processing
        endpoint = 'http://54.164.253.138:8080/api/v1/dags/midterm_dag/dagRuns'
        subprocess.run([
            'curl',
            '-X',
            'POST',
            endpoint,
            '-H',
            'Content-Type: application/json',
            '--user',
            'airflow:airflow',
            '--data',
            data
        ])
        print('Files are sent to Airflow')
    else:
        # If any required file is missing in the S3 bucket, send an alert email
        send_email()
