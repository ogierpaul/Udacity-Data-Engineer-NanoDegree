import logging

import boto3
from botocore.exceptions import ClientError


def upload_file(aws_access_key_id, aws_secret_access_key, region_name, file_path, bucket, object_name):
    s3_client = boto3.client('s3',
                             region_name=region_name,
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
        print(f'{file_path} uploaded to s3://{bucket}/{object_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file_config(config, file_path, bucket, object_name):
    """Upload a file to an S3 bucket
    :param config: config file
    :param file_path: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name.
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_path

    # Upload the file
    o = upload_file(region_name=config.get("REGION", "REGION"),
                aws_access_key_id=config.get("AWS", "KEY"),
                aws_secret_access_key=config.get("AWS", "SECRET"),
                bucket=bucket,
                file_path=file_path,
                object_name=object_name
                )
    return o


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


def concat_s3_path(bucket, folder, filename):
    s3_key = '/'.join(['s3:/', bucket.strip('/'), folder.strip('/'), filename.strip('/')])
    return s3_key