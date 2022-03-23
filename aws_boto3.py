import boto3

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

    :param file_name: File to upload
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


if __name__ == "__main__":
    print(s3_list_files())
    s3_upload_file("/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/random-meter-ids.csv", 
                    BUCKET_NAME, 
                    object_name="data/random-meter-ids")
