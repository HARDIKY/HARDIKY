print('importing libraries..')
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from awsglue.job import Job
import boto3
from pyspark.sql.functions import date_sub
from datetime import datetime, timedelta
import dateutil
import boto3
import json
from botocore.exceptions import ClientError
print('libraries imported..!')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bkt_name', 'amc_s3_folder', 'wealth_s3_folder', 'aam_s3_folder', 'connection_name', 'snfl_secret'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

IST = dateutil.tz.gettz('Asia/Kolkata')
yesterday = datetime.now(tz=IST) - timedelta(days=1)

# Enhanced timestamp formats
date_str = yesterday.strftime('%Y%m%d')
date_str_ = yesterday.strftime('%Y-%m-%d')
# Add timestamp with hour, minute, second
timestamp_str = yesterday.strftime('%Y%m%d_%H%M%S')
# ISO format timestamp for more precise tracking
iso_timestamp = yesterday.strftime('%Y-%m-%dT%H:%M:%S')

bkt_name = args['bkt_name']
amc_s3_folder = args['amc_s3_folder']
wealth_s3_folder = args['wealth_s3_folder']
aam_s3_folder = args['aam_s3_folder']
snfl_secret = args['snfl_secret']
connection_name = args['connection_name']


secretsmanager_client = boto3.client('secretsmanager')
secret_response = secretsmanager_client.get_secret_value(SecretId=snfl_secret)
snowflake_details = json.loads(secret_response['SecretString'])
sfDatabase = snowflake_details['sfDatabase']
sfSchema = snowflake_details['sfSchema']
sfWarehouse = snowflake_details['sfWarehouse']
sfUser = snowflake_details['sfUser']
sfURL = snowflake_details['sfUrl']
pem_private_key = snowflake_details['pem_private_key']

print(' secret manager for snowflake connected: ', sfDatabase, sfSchema, sfWarehouse, sfUser, sfURL)

sf = {
        "sfUser": sfUser,
        "sfURL": sfURL,
        "sfWarehouse": sfWarehouse,
        "sfDatabase": sfDatabase,
        "sfSchema": sfSchema,
        "pem_private_key": pem_private_key
}


amc_query = "SELECT * FROM IIFLW_INT_DB.BI_REPORTING.AMC_INC_EMPLOYEE_TXN"
wealth_query = "SELECT * FROM IIFLW_INT_DB.BI_REPORTING.WEALTH_INC_EMPLOYEE_TXN"
aam_query = "SELECT * FROM IIFLW_INT_DB.BI_REPORTING.AAM_INC_EMPLOYEE_TXN"


def snfl_df(query):
    try:
        snfl_df = spark.read.format("snowflake").options(**sf).option("query", query).load()
        return snfl_df
    except Exception as e:
        print("GLUE ERROR: Error in retrieving data from Snowflake:", str(e))
        raise


def sf_write(bkt_name, df, s3_folder, filename):
    # Create S3 path with date folder
    s3_path = f's3://{bkt_name}/{s3_folder}/{date_str_}/'

    # Write to S3 with coalesce to single file
    df.coalesce(1).distinct().write.mode("overwrite")\
        .option('header', 'true')\
        .option('escape', '"')\
        .option('quote', '"')\
        .csv(s3_path)

    # Enhanced filename with full timestamp
    desired_filename = f'{filename}_{timestamp_str}.csv'

    s3_client = boto3.client('s3')
    try:
        # List objects in the S3 path
        response = s3_client.list_objects_v2(
            Bucket=bkt_name,
            Prefix=f'{s3_folder}/{date_str_}/'
        )

        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                new_key = f'{s3_folder}/{date_str_}/{desired_filename}'

                # Copy to new filename
                s3_client.copy(
                    {'Bucket': bkt_name, 'Key': key},
                    bkt_name,
                    new_key
                )

                # Add metadata with timestamps
                s3_client.put_object_tagging(
                    Bucket=bkt_name,
                    Key=new_key,
                    Tagging={
                        'TagSet': [
                            {'Key': 'ProcessDate', 'Value': date_str_},
                            {'Key': 'ProcessTimestamp', 'Value': iso_timestamp},
                            {'Key': 'JobName', 'Value': args['JOB_NAME']},
                            {'Key': 'FileType', 'Value': filename}
                        ]
                    }
                )

                # Delete the original file
                s3_client.delete_object(Bucket=bkt_name, Key=key)
                print(f"File renamed to: {new_key} with timestamp: {timestamp_str}")

    except ClientError as e:
        print(f"Error renaming files: {e}")

    return sf_write


# Add processing start time
processing_start = datetime.now(tz=IST)
print(f"Processing started at: {processing_start.strftime('%Y-%m-%d %H:%M:%S IST')}")

# Call the snfl_df function to retrieve data and write the DataFrame to S3
print("Processing AMC Employee Transaction data...")
df1 = snfl_df(amc_query)
sf_write(bkt_name, df1, amc_s3_folder, 'AMC_Employee_Trxn')

print("Processing Wealth Employee Transaction data...")
df2 = snfl_df(wealth_query)
sf_write(bkt_name, df2, wealth_s3_folder, 'Wealth_Employee_Trxn')

print("Processing AAM Employee Transaction data...")
df3 = snfl_df(aam_query)
sf_write(bkt_name, df3, aam_s3_folder, 'AAM_Employee_Trxn')

# Add processing end time
processing_end = datetime.now(tz=IST)
processing_duration = processing_end - processing_start

print(f"Processing completed at: {processing_end.strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"Total processing time: {processing_duration}")

job.commit()
