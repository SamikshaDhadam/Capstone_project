import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import os
def upload_file_to_s3(local_file_path, bucket_name, s3_file_key):
    """
    Uploads a file to an S3 bucket.
    :param local_file_path: Path to the local file
    :param bucket_name: Name of the S3 bucket
    :param s3_file_key: S3 object key (path within the bucket)
    """
    # Create an S3 client
    s3_client = boto3.client('s3',
        aws_access_key_id='AKIAXXXXXXXXXXXXLMXL',      
        aws_secret_access_key='q+/wsNXXXXXXXXXXXXXXXXXVCeKNg' 
    )
    try:
        # Upload file to S3 bucket
        s3_client.upload_file(local_file_path, bucket_name, s3_file_key)
        print(f"File '{local_file_path}' uploaded to bucket '{bucket_name}' as '{s3_file_key}'.")
    except FileNotFoundError:
        print(f"The file '{local_file_path}' was not found.")
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")


def upload_files_from_folder_to_s3(folder_path, bucket_name, s3_folder_key):
    """
    Uploads all files from a local folder to an S3 bucket.
    """
    # Check if the folder path exists and is accessible
    if not os.path.exists(folder_path):
        print(f"Error: The folder '{folder_path}' does not exist or is not accessible.")
        return

    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        local_file_path = os.path.join(folder_path, filename)

        # Check if the path is a file and not a directory
        if os.path.isfile(local_file_path):
            s3_file_key = os.path.join(s3_folder_key, filename)
            print(f"Uploading '{local_file_path}' to S3 as '{s3_file_key}'...")
            upload_file_to_s3(local_file_path, bucket_name, s3_file_key)
        else:
            print(f"Skipped: '{local_file_path}' is not a file.")        
#upload transactions.csv file
folder_path = r'C:\Users\samikshav\Desktop\Snowflake\last_try' 
bucket_name = 'sami-s3-bucket' 
s3_folder_key = 'Data' 

# Upload all files from the folder to S3
upload_files_from_folder_to_s3(folder_path, bucket_name, s3_folder_key)