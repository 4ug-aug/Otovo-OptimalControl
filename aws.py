import asyncio
from aioaws.s3 import S3Client, S3Config
from httpx import AsyncClient
import configparser

config_parser = configparser.ConfigParser()
config_parser.read("config/aws.config")
aws_config = config_parser["AWS_BUCKET"]
print(config_parser.sections())

# AWS Creds
aws_config = S3Config(**aws_config)

async def s3_list_files(client: AsyncClient):
    """Get list of files

    Args:
        client (AsyncClient): AWS client

    Returns:
        list: List of files in bucket
    """
    s3 = S3Client(client, aws_config)
    files = [f async for f in s3.list()]

    return files

async def s3_get_download_url(key, client: AsyncClient):
    s3 = S3Client(client, aws_config)
    # s3.download_file(key,key,'data/'+key)
    url = s3.signed_download_url(key)
    return url

async def s3_upload_file(client: AsyncClient):
    s3 = S3Client(client, aws_config)
    #TODO Still missing
    # await s3.upload("test.str", "Some test".encode("utf-8"))
    return 


async def main():
    async with AsyncClient(timeout=30) as client:
        await s3_list_files(client)
        await s3_get_download_url("grid-metering-point-dump-plus-zipcode.csv.gz", client)
        # s3_upload_file(client)

if __name__ == "__main__":
    print(asyncio.run(main()))
    

    # Upload a new file to aws
    # await s3_client.upload("test.str", "Some test".encode("utf-8"))