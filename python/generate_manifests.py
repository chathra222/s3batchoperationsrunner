import urllib.parse
import re
import boto3

input_file = '0_input/bucket_prefix_list.txt'

def check_filepath_syntax(file_path):
    searchObj = re.search('[:*?|<>]', file_path)
    if searchObj:
        return False
    else:
        return True

# Manifest style file with /dirname/url+file+name+ style (URL encoded)
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html
def s3_urlplus_filename(original_filename):
    chopped_string_list = original_filename.split('/')
    encoded_string_list = []
    for name in chopped_string_list:
        encoded_string_list.append(urllib.parse.quote_plus(name))
    return "/".join(encoded_string_list)

# Directly feed data from s3 list object into manifest file
def generate_s3_manifest_csv2(s3_bucketname, s3_prefix, manifest_s3_filename):
    s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=s3_bucketname, Prefix=s3_prefix)

    for page in response_iterator:
        contents = page['Contents']

        with open(manifest_s3_filename, 'a') as s3_f:
            
            for s3_object in contents:
                if check_filepath_syntax(s3_object['Key']):
                    encoded_filename = s3_urlplus_filename(s3_object['Key'])
                    s3_f.write(f"{s3_bucketname},{encoded_filename}\n")
    
# Abstraction of manifest files generation code
def generate_manifest_files(proj_name):
    src_bucketname = (proj_name.split(","))[0]
    src_prefix = (proj_name.split(","))[1]
    src_prefix_forfilename = ((proj_name.split(","))[1]).replace("/","")
    manifest_filename = f"0_output/MANIFEST-{src_bucketname}-{src_prefix_forfilename}.csv"
    print(f' - Generating manifest file... {manifest_filename}')
    generate_s3_manifest_csv2(src_bucketname, src_prefix, manifest_filename)

# You need to put list of bucket and prefix pair(bucket,prefix) in input file
def create_manifest_from_file(filename):
    print(f"Input filename: {filename}")
    with open(filename, 'r') as input_f:
        for proj_name in input_f.readlines():
            generate_manifest_files(proj_name.strip())

create_manifest_from_file(input_file)