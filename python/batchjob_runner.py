# upload the manifest to S3 - Done
# get-object etag - Done
# create job based on based on directory


import logging
import boto3
from botocore.exceptions import ClientError
import os
import re
import sys
import uuid
import configparser

from botocore.config import Config

config_parser = configparser.ConfigParser()
config_parser.read('config.ini')
configs = config_parser['config']
boto_config = Config(
    region_name = configs['region'],
)
s3_client = boto3.client('s3',config=boto_config)
manifest_bucket_name = configs['manifest_bucket_name']
reports_bucket_name = configs['reports_bucket_name']
S3BatchCopyLambdafunction = configs['s3_batchops_lambda_arn']
S3BatchOperationsServiceIamRole = configs['s3_batchops_role_arn']
account_id = configs['account_id']
s3_arn_prefix = "arn:aws:s3:::"


def get_object_info(bucket, key):

    obj_info = {}
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key)
    obj_info['ETag'] = response['ETag']
    obj_info['arn'] = s3_arn_prefix+bucket+f"/"+key
    obj_info['key'] = key
    print(obj_info['arn'])
    return obj_info


def upload_file(file_name, bucket, key):
    try:
        response = s3_client.upload_file(file_name, bucket, key)
        return get_object_info(bucket, key)
    except ClientError as e:
        logging.error(e)


def upload_manifest_to_s3(projectname):
    print("Started uploading")
    src_bucketname = (projectname.split(","))[0]
    src_prefix = (projectname.split(","))[1]
    print(src_bucketname)
    print(src_prefix)
    src_prefix_forfilename = (src_prefix).replace("/","-")
    print(src_prefix_forfilename)
    filename = f"MANIFEST-{src_bucketname}-{src_prefix}.csv"
    manifest_srcfilename = f"0_output/MANIFEST-{src_bucketname}-{src_prefix}.csv"
    key = filename
    print(
        f"Uploading manifest: {filename} to {manifest_bucket_name}/{key}")
    return upload_file(manifest_srcfilename, manifest_bucket_name, key)

def create_s3_batchjob(project_name, object_arn, etag):
    src_bucketname = (project_name.split(","))[0]
    src_prefix = (project_name.split(","))[1]
    client = boto3.client('s3control',config=boto_config)
    batch_ops_role_arn = S3BatchOperationsServiceIamRole
    lambda_arn = S3BatchCopyLambdafunction
    key_prefix = ""
    report_prefix = key_prefix+f"MANIFEST_JOBS"
    description = (src_bucketname+src_prefix).replace("/","-") + " S3 batchjob"
    print(account_id)
    print(lambda_arn)
    report_bucket = s3_arn_prefix+reports_bucket_name
    print(report_bucket)

    response = client.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={
            'LambdaInvoke': {
                'FunctionArn': lambda_arn
            },
        },
        Report={
            'Bucket': report_bucket,
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'Prefix': report_prefix,
            'ReportScope': 'FailedTasksOnly'
        },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': [
                    'Bucket', 'Key'
                ]
            },
            'Location': {
                'ObjectArn': object_arn,
                'ETag': etag
            }
        },
        ClientRequestToken=str(uuid.uuid4()),
        Description=description,
        Priority=123,
        RoleArn=batch_ops_role_arn,
        Tags=[
        ]
    )

with open('0_input/bucket_prefix_list.txt', 'r') as input_f:
    for proj_name in input_f.readlines():
        print(proj_name)
        object_details = upload_manifest_to_s3(
            proj_name.strip())
        create_s3_batchjob(proj_name.strip(
        ), object_details['arn'], object_details['ETag'])
