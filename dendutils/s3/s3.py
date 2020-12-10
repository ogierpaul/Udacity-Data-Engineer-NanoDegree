import logging

import boto3
from botocore.exceptions import ClientError


def upload_file(config, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param config: config file
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3',
                            region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET"))
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(f'{file_name} uploaded to s3://{bucket}/{object_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def s3_list_objects(s3c, bucket, prefix):
    """

    Args:
        s3c (boto3.Client): s3 CLIENT
        bucket (str): bucket
        prefix (str): prefix


    Returns:
        list
    """
    m = []
    for key in s3c.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
        k = key['Key']
        m.append(k)
    return m