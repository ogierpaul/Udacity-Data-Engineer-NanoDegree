import requests
import pandas as pd
import json
import boto3
import logging

def get_myip():
    """
    Obtain the IP of the machine where the python script is launched
    Add a /32 after this IP
    Returns:
        str: (Example: "139.59.2.125/32")
    """
    r = requests.get('http://checkip.amazonaws.com/')
    r = r.text.rstrip('\n')
    r += '/32'
    return r


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

def create_iam_role(iam, iam_role_name, policy):
    """
    Create an IAM role for the Redshift Cluster (The Principal), to allow AmazonS3ReadOnlyAccess (the policy)
    Args:
        iam (bot3.client): boto3 IAM Object
        iam_role_name (str): name to be given to the IAM role
        policy(str): AWS policy (Like: "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    Returns:
        roleIARN
    """

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    r  = iam.attach_role_policy(RoleName=iam_role_name,
                                PolicyArn=policy
                                )['ResponseMetadata']['HTTPStatusCode']
    print(f"HTTPS Status code:{r}")
    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']

    print(roleArn)
    return roleArn

def open_ports(config, cluster_properties):
    """
    Update clusters security group to allow access through redshift port
    Authorize ingres on IP of the executable
    Args:
        ec2 (boto3.client): ec2 client
        cluster_properties (pd.Series): Pandas Series
        port (str): port of the database

    Returns:

    """

    print("2.3 Opening port of the cluster")
    myip = get_myip()
    print('MyIP: Cidr IP block of executable:', myip)
    ec21 = boto3.client('ec2',
                         region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")

    )
    ec22 = boto3.resource('ec2',
                        region_name=config.get("REGION", "REGION"),
                        aws_access_key_id=config.get("AWS", "KEY"),
                        aws_secret_access_key=config.get("AWS", "SECRET")
                        )
    try:
        vpc = ec22.Vpc(id=cluster_properties['VpcId'])
        print('VpcId:', cluster_properties['VpcId'])
        # Sg = list(vpc.security_groups.all())[0]
        sg_id = cluster_properties['VpcSecurityGroups'][0]['VpcSecurityGroupId']
        print('Sg:', sg_id)
        port= config.get("DB", "DB_PORT")
        sg = ec21.authorize_security_group_ingress(
            GroupId=sg_id,
            CidrIp=myip,
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)

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


