import boto3
import time
import sys
from datetime import datetime, date, timedelta
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

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_client = boto3.client(service_name='s3', region_name='us-east-1') 

args = getResolvedOptions(sys.argv, ['JOB_NAME','SUBJOB_ID','SOURCE_PREFIX','TRIGGER_PREFIX','SOURCE_EXTENSION','TRIGGER_EXTENSION','DATE_FORMATE','HEADERS',
        'SOURCE_FOLDER','INVOICE_MONTH','INVOICE_MONTH_FORMATE','INVOICE_MONTH_DIFF', 'BATCH_RUNDATE'])
        
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Job Start time:", dt_start)
#################################################################

prefix=''
env=''
if (args['JOB_NAME'].__contains__('hsfr-d')):
    prefix='hsfr-d'
    env='dev'
    table='hsfr_d'
elif (args['JOB_NAME'].__contains__('hsfr-s')):
    prefix='hsfr-s'
    env='sys'
    table='hsfr_s'
elif (args['JOB_NAME'].__contains__('hsfr-i')):
    prefix='hsfr-i'
    env='int'
    table='hsfr_i'
elif (args['JOB_NAME'].__contains__('hsfr-v')):
    prefix='hsfr-v'
    env='pvs'
    table='hsfr_v'
elif (args['JOB_NAME'].__contains__('hsfr-u')):
    prefix='hsfr-u'
    env='uat/rel'
    table='hsfr_u'
elif (args['JOB_NAME'].__contains__('hsfr-f')):
    prefix='hsfr-f'
    env='pfx'
    table='hsfr_f'
elif (args['JOB_NAME'].__contains__('hsfr-p')):
    prefix='hsfr-p'
    env='prd'
    table='hsfr_p'

dataBase="{}-db-audit-data-master".format(prefix)
jobvalidationRulesTableName="{}_audit_job_validation_rules".format(table)
jobvalidationResultTableName="{}_audit_job_validation_results".format(table)
filePath="s3a://{0}-641158851584-audit-bucket/{1}-audit-job-validation-results/batchdate=".format(prefix,prefix)+str(datetime.now().strftime("%Y-%m-%d"))
bucket = "{0}-641158851584-src-bucket".format(prefix)

df = glueContext.create_dynamic_frame.from_catalog(database = dataBase, table_name = jobvalidationRulesTableName, transformation_ctx = "df").toDF()
df.createOrReplaceTempView("job_validation_rules")
list_job_validn_rules = spark.sql('select rule_id,validation_rule from job_validation_rules where subjob_id = {0} and active_flag = "y" order by rule_id'.format(args['SUBJOB_ID'])).collect()

client = boto3.client(service_name='glue', region_name='us-east-1', endpoint_url='https://glue.us-east-1.amazonaws.com')


if args['BATCH_RUNDATE'] != None:
    argbatchdate = args['BATCH_RUNDATE']
    format = "%Y-%m-%d"
    argbatchdate = datetime.strptime(argbatchdate, format)
    argbatchdate = argbatchdate.strftime("%Y-%m-%d")
    argbatchdate1 = str(argbatchdate)
else:
    argbatchdate = str(datetime.now().strftime("%Y-%m-%d"))
    argbatchdate1 = str(argbatchdate)


try:
    response = client.get_table(DatabaseName=dataBase, Name=jobvalidationResultTableName)
except client.exceptions.EntityNotFoundException:
    job_runid = 1
    print("job_runid:",job_runid)
else:
    validation_results_df = glueContext.create_dynamic_frame.from_catalog(database = dataBase, table_name = jobvalidationResultTableName, transformation_ctx = "job_validation_status").toDF() #NEW
    validation_results_df.createOrReplaceTempView("job_validation_status")
    df_paramDetail = spark.sql("select CASE WHEN max(job_runid) IS NULL THEN '0' ELSE max(job_runid) END as job_runid from job_validation_status where subjob_id={0} and batch_rundate= '{1}'".format(args['SUBJOB_ID'],argbatchdate1)).collect()
    job_runid = int(df_paramDetail[0]['job_runid'])
    print(df_paramDetail)
    job_runid = job_runid + 1
    print("job_runid:",job_runid)
#################################################################

jobStatusList = [("rule_id","subjob_id","batch_rundate","job_runid","validation_rule","validation_result","source_cnt","target_cnt","comments")]
jobStatusListColumns = ["rule_id","subjob_id","batch_rundate","job_runid","validation_rule","validation_result","source_cnt","target_cnt","comments"]

def validate_date(date_text, date_format):
    try:
        datetime.strptime(date_text, date_format)
        return True
    except ValueError:
        print("Incorrect data format, should be "+date_format)
        return False

def send_notification(job_name, name ,error_msg):
    sns_client = boto3.client('sns')
    msg_str = "Pre-Ingestion job has failed to process {0} file because of the error: \n {1}".format(name, error_msg)
    sub_str = "Pre-Ingestion job Failed and {0} job is Stopped".format(job_name)
    sns_response = sns_client.publish(TopicArn='arn:aws:sns:us-east-1:641158851584:send-email',
                                  Message = msg_str,
                                  Subject=sub_str)

def move_file(source_bucket, dest_bucket, source_folder, dest_folder, filename):
    source_path = source_folder + '/' + filename
    dest_path = 'invalid-data' + '/' + source_folder + '/' + filename
    
    s3_client.copy_object(CopySource = {'Bucket': source_bucket, 'Key': source_path},
                          Bucket = dest_bucket, Key = dest_path)
    s3_client.delete_object(Bucket=source_bucket, Key=source_path)

def format_check(file_name, prefix, date_format, extension):
    msg = ''
    #check prefix
    if(file_name[:len(prefix)] == prefix):
        print('Correct prefix')
    else:
        msg = 'Incorrect prefix'
        return False, msg
    #check date-format
    current_date = datetime.now().strftime(date_format)
    file_date = file_name[len(prefix)+1:len(prefix)+len(current_date)+1]
    if(validate_date(file_date, date_format)):
        print("Correct date format")
    else:
        msg = 'Incorrect date format'
        return False, msg
    #check extension
    if(file_name[len(prefix)+len(current_date)+1:] == extension):
        print("Correct extension")
        msg = 'format check passed'
        return True, msg
    else:
        msg = 'Incorrect extension'
        return False, msg

def check_trigger_file(bucket, source_folder, prefix, date_format, extension, rule_id, validation_rule):
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket,Prefix=source_folder)
    except ClientError:
        msg = 'No such bucket'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
        send_notification('Pre-Ing-Glue_Job', prefix , 'Incorrect S3 Bucket configured')
        return False
    if('Contents' in objs):
        flag = False
        for file in objs['Contents']:
            file_name = file["Key"].split("/")[1]
            if(file_name is not '' and file_name[-len(extension):] == extension):
                flag = True
                result, msg = format_check(file_name, prefix, date_format, extension)
                if(not result):
                    jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
                    send_notification('Pre-Ing-Glue_Job', prefix , 'Trigger file with incorrect format ' + msg)
                    return False
                else:
                    jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Pass","","",msg)])
                    return True
        if(not flag):
            msg = 'Extension is incorrect'
            jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
            send_notification('Pre-Ing-Glue_Job', prefix , 'Trigger file with incorrect format')
            return False
    else:
        msg = 'No such source folder'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
        send_notification('Pre-Ing-Glue_Job', prefix , 'Trigger file with incorrect format')
        return False

def check_data_files(bucket, source_folder, prefix, date_format, extension, rule_id, validation_rule):
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket,Prefix=source_folder)
    except ClientError:
        msg = 'No such bucket'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
        send_notification('Pre-Ing-Glue_Job', prefix , 'Incorrect S3 Bucket configured')
        return False
    if('Contents' in objs):
        flag = False
        for file in objs['Contents']:
            file_name = file["Key"].split("/")[1]
            print("File: ", file_name)
            if(file_name is not ''):
                trg = args['TRIGGER_EXTENSION']
                if(file_name[-len(trg):] != trg):
                    result, msg = format_check(file_name, prefix, date_format, extension)
                if(not result):
                    jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
                    send_notification('Pre-Ing-Glue_Job', prefix , 'Data file with incorrect format')
                    move_file(bucket, bucket, source_folder, source_folder, file_name)
                elif(file_name[-len(trg):] != trg):
                    print("All good")
        bucket_listing = s3_client.list_objects_v2(Bucket=bucket,Prefix=source_folder)
        if(len(bucket_listing['Contents']) > 2):
            msg = 'Data file check completed'
            jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Pass","","",msg)])
            return True
        else:
            msg = 'All data files are bad'
            jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
            send_notification('Pre-Ing-Glue_Job', prefix , 'Data file with incorrect format')
            return False
    else:
        msg = 'No such source folder'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
        send_notification('Pre-Ing-Glue_Job', prefix , 'Data file with incorrect format')
        return False
        
def check_invoice_month(invoice_date, date_format, diff_value, rule_id, validation_rule):
    current_month = datetime.now().strftime("%m")
    invoice_dt = datetime.strptime(invoice_date, date_format)
    invoice_month = invoice_dt.strftime("%m")
    if(int(current_month)-int(invoice_month) == int(diff_value)):
        msg = 'invoice month is correct'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Pass","","",msg)])
        return True
    else:
        msg = 'invoice month is out of range'
        jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
        return False

def check_header_and_size(header, path):
    datasource0 = glueContext.create_dynamic_frame.from_options('s3',
                                                            {'paths': [path],'compression':'bzip'},
                                                            'csv',
                                                            {'withHeader': True}) #,'separator': separator
    ds_file = datasource0.toDF()
    cols_list = ds_file.columns
    header_list = '|'.join([str(elem) for elem in cols_list])
    if(header_list == header):
        msg = 'Header are correct'
    else:
        msg = 'Header are incorrect'
        return False, msg
    size = ds_file.count()
    if(size > 0):
        msg = 'Header and file size is good'
        return True, msg
    else:
        msg = 'File does not contains data'
        return False, msg


def check_header_and_size_for_all(bucket, source_folder, header, rule_id, validation_rule):
    bucket_listing = s3_client.list_objects_v2(Bucket=bucket,Prefix=source_folder)
    flag = False
    for item in bucket_listing['Contents']:
        if(item['Key'].__contains__(args['SOURCE_EXTENSION'])):
            path = 's3://' + bucket + '/' + item['Key']
            print(path)
            result, msg = check_header_and_size(header, path)
            if(result):
                jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Pass","","",msg)])
                flag = True
            else:
                jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
    return flag


def check_size(path, count):
    datasource0 = glueContext.create_dynamic_frame.from_options('s3',
                                                            {'paths': [path],'compression':'bzip'},
                                                            'csv',
                                                            {'withHeader': True})
    ds_file = datasource0.toDF()
    size = ds_file.count()
    print(path)
    if(size == int(count)):
        msg = 'Record count is correct'
        return True, msg
    else:
        msg = 'Record count is incorrect'
        return False, msg


def check_record_count(bucket, source_folder, rule_id, validation_rule):
    bucket_listing = s3_client.list_objects_v2(Bucket=bucket,Prefix=source_folder)
    trg_path = ''
    
    for item in bucket_listing['Contents']:
        if(item['Key'].__contains__(args['TRIGGER_EXTENSION'])): 
            trg_path = 's3://' + bucket + '/' + item['Key']
    
    df = spark.read.format("text").load(trg_path).collect()
    rec_count_dict = {}
    for record in df:
        fileDetails = record['value'].split('|')
        key = fileDetails[0]
        value = fileDetails[1]
        rec_count_dict[key] = value
    
    flag = False
    for item in bucket_listing['Contents']:
        if(item['Key'].__contains__(args['SOURCE_EXTENSION'])): 
            path = 's3://' + bucket + '/' + item['Key']
            file_name = item['Key'].split("/")[1]
            count = rec_count_dict[file_name]
            result, msg = check_size(path, count)
            if(result):
                print("Passed")
                jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Pass","","",msg)])
                flag = True
            else:
                print("Failed")
                jobStatusList.extend([(str(rule_id), args['SUBJOB_ID'], str(datetime.now().strftime("%Y-%m-%d")), str(job_runid),str(validation_rule),"Fail","","",msg)])
    return flag

    

def writeFinalOutPut(jobStatusList,jobStatusListColumns):
    jobStatus = spark.createDataFrame(data=jobStatusList, schema=jobStatusListColumns)
    jobStatus.coalesce(1).write.format("csv").option("header", "false").mode("append").save(filePath)

#################################################################

if(len(list_job_validn_rules) > 0):
    for row in list_job_validn_rules:
        print("\n ", row['rule_id'], " - ", row['validation_rule'])
        if(row['validation_rule'] == 'check_trigger_file'):
            res = check_trigger_file(bucket, args['SOURCE_FOLDER'], args['TRIGGER_PREFIX'], args['DATE_FORMATE'], args['TRIGGER_EXTENSION'], row['rule_id'], row['validation_rule'])
            if(not res):
                break
        if(row['validation_rule'] == 'check_data_file'):
            res = check_data_files(bucket, args['SOURCE_FOLDER'], args['SOURCE_PREFIX'], args['DATE_FORMATE'], args['SOURCE_EXTENSION'], row['rule_id'], row['validation_rule'])
            if(not res):
                break
        if(row['validation_rule'] == 'check_header'):
            res = check_header_and_size_for_all(bucket, args['SOURCE_FOLDER'], args['HEADERS'], row['rule_id'], row['validation_rule'])
            if(not res):
                break
        if(row['validation_rule'] == 'check_file_size'):
            res = check_record_count(bucket, args['SOURCE_FOLDER'], row['rule_id'], row['validation_rule'])
            if(not res):
                break
        if(row['validation_rule'] == 'check_invoice_month'):
            res = check_invoice_month(args['INVOICE_MONTH'], args['INVOICE_MONTH_FORMATE'], args['INVOICE_MONTH_DIFF'], row['rule_id'], row['validation_rule'])
            if(not res):
                break
        

############################################################ Write a file ######################################################################

writeFinalOutPut(jobStatusList, jobStatusListColumns)    

########################################################### Log end time #######################################################################

dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Job End time:", dt_end)
job.commit()



