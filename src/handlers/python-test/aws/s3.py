import boto3 # Import the boto3 library, which provides an interface to AWS services including S3

def upload_file_to_s3(file_name, bucket, object_name=None): # Uploads a file to an AWS S3 bucket
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3') # Create an S3 client using boto3. This client provides methods to interact with S3
    
    # Upload the file to the specified S3 bucket. 'file_name' will be the local path to the file,
    # 'bucket' will be the name of the S3 bucket, and 'object_name' will be the name for the file in S3
    response = s3_client.upload_file(file_name, bucket, object_name)

    return response
