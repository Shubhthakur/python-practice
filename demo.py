import boto3
import time
import sys
from datetime import datetime, date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import row_number,rank, desc, col,when,to_date,length, trim
import pyspark.sql.utils
from botocore.exceptions import ClientError
from io import BytesIO
import gzip


def check_file_availablity(bucket, key):
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        if(response['ResponseMetadata']['HTTPStatusCode'] == 200):
            return response['Body']
        else:
            print('Log:')
            return False
    except ClientError as ex:
        if(ex.response['Error']['Code'] == 'NoSuchKey'):
            print('Log: No object found')
            return False
        
def check_size_and_header(response, header):
    bytes_io = BytesIO(response.read())
    data = gzip.GzipFile(fileobj=bytes_io)
    content = data.read().decode('utf-8')
    if(content[-1]=='\n'):
        content = content[:-1]
    rows = content.split('\n')
    if(len(rows)>1):
        print('log: success')
    else:
        print('log: fail')
        return False
    head_row = rows[0].replace(',','|')
    if(head_row == header):
        return True
    else:
        return False

    
    
def check_invoice_month(invoice_date, date_format, diff_value):
    current_month = datetime.now().strftime("%m")
    invoice_dt = datetime.strptime(invoice_date, date_format)
    invoice_month = invoice_dt.strftime("%m")
    if(current_month-invoice_month == diff_value):
        print()
    else:
        print()
