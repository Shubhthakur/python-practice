from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext

'''
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
'''

sc = SparkContext()
glueContext = GlueContext(sc)
arg_SRC_DB_NAME = 'db-mocked-data-master'
arg_SRC_TABLE_NAME = 'manufacturer_otr'


df = glueContext.create_dynamic_frame.from_catalog(database = arg_SRC_DB_NAME, table_name = arg_SRC_TABLE_NAME).toDF()
print("Data frame size : ", df.count())
print("Data frame Columns : ", df.columns)
df.printSchema()
