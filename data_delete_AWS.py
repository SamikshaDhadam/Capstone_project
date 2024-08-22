import boto3

# Initialize S3 client
s3_client = boto3.client('s3')

# Define your bucket name and the prefix 
bucket_name = 'sami-s3-bucket'
prefix = 'Data/' 

# List and delete all files under the prefix
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Deleted {obj['Key']} from bucket {bucket_name}.")
else:
    print("No files found with the given prefix.")
