# S3 Batch Operations manifest generator and job runner
This solution will help to create s3 batch operation manifests and run multiple of jobs programmatically. This is especially useful when you want to transfer part of the bucket(a folder/prefix) to another bucket using s3 batch operations.
## Pre-requisites
Make sure you already have source bucket,destination bucket and manifest bucket and report buckets in place.
You may use the same bucket for to keep manifest and reports.
##  1. Deploy the cloudformation template
    You have to change following parameters in cloudformation according to your needs:
        - BucketForCopySource
        - BucketForCopyDestination
        - BucketForManifestOrInventory
        - BucketForBatchOpsReport

    This template will create:
        - Lambda function
        - IAM Role for Lambda function
        - IAM Role for S3 batch operations job

##  2. Update the config.ini file
    - You can the get arns of lambda function and s3 batch operations role after running step 1.
##  2. Creating the Mainfest (CSV file)
       
    - Update the Bucket and prefix pairs in bucket_prefix_list.txt file.
      In this file you can include bucket and prefix pairs in following format:
      
      <bucket_name>,<prefix name>
      
    - run follwing command
     
      python3 python/generate_manifests.py
      
      This script will generate a CSV file where Object keys URL-encoded(+ encoded)
     
##  3. Upload the generated Manifests and execute S3 batch job runner to create S3 batch operation jobs
    - run follwing command
   
      python3 python/batchjob_runner.py

     
