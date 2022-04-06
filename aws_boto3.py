import boto3
from pprint import pprint

# Download aws CLI: https://aws.amazon.com/cli/
# Then run:
# aws configure
# And enter credentials 

BUCKET_NAME = 'otovo-student-optimalcontrol'
s3 = boto3.client("s3")

def s3_list_files():
    """ List files in AWS Bucket
    
    Returns:
        list of file names from bucket
    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)

    files = list()

    for object_summary in my_bucket.objects.all():
        files.append(object_summary.key)
    
    return files

def s3_download_file(obj_name, fp):
    """Download a file from an S3 bucket

    :param obj_name:        File to download
    :param fp:              Name of new file
    """
    s3.download_file(BUCKET_NAME, obj_name, fp)
    
def s3_upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    from botocore.exceptions import ClientError
    import os
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def s3_delete_file(file_name):
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)

# Run script with arguments
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Download files from AWS S3')
    parser.add_argument('-m', '--mode', type=str, default='download', help='Mode of operation: download, upload', required=True)
    parser.add_argument('--file', type=str, help='File to download', required=False)
    parser.add_argument('--bucket', type=str, help='Bucket to download from', required=False)
    parser.add_argument('--out', type=str, help='Output file name')
    args = parser.parse_args()

    if args.mode == "download" and args.file is not None:
        s3_download_file(args.file, args.out)

    elif args.mode == "upload" and args.file is not None:
        s3_upload_file(args.file, args.bucket, args.out)

    elif args.mode == "list": 
        print(*s3_list_files(),sep='\n')
