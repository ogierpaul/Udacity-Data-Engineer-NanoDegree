import time
import psycopg2
from dendutils.aws import open_ports, create_iam_role, get_myip
import boto3
import pandas as pd
from awscli.errorhandler import ClientError

def show_all_instances_status(e):
    """

    Args:
        e (boto3.client):

    Returns:
        list of tuples (instance_id, state_name)
    """
    o = [(c['Instances'][0]['InstanceId'] , c['Instances'][0]['State']['Name']) for c in
     e.describe_instances()['Reservations']]
    return o

def filter_per_tag(e, key, value):
    """

    Args:
        e: boto3 ec2 client
        key (str): Tag key
        value (str): Tag Value

    Returns:
        list: list of strings , instances ids
    """

    query =[{
    "Name":"tag-key",
    "Values":[key]
    }]
    res = []
    props = e.describe_instances(Filters=query)['Reservations']
    for i in props:
        j = i['Instances']
        j_id = j[0]['InstanceId']
        #TODO: Change elsewhere
        i_sg_id = j[0]['SecurityGroups'][0]['GroupId']
        for t in j[0]['Tags']:
            if t.get("Key") == key and t.get("Value") == value:
                res.append(j_id)
    return res


def create_vm(e, IMAGE_ID, KEY_NAME, INSTANCE_TYPE, SECURITY_GROUP, IAM_NAME, TAG_KEY, TAG_VALUE ):
    """
    See here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Subnet.create_instances
    Args:
        e (boto3.resource): boto3 resource
        IMAGE_ID (str): VM (AMI) image Id
        KEY_NAME (str): PEM key name
        INSTANCE_TYPE (str): instance type , ex t2.micro
        SECURITY_GROUP (str): Security Group Name
        IAM_NAME (str): Iam Role Name
        TAG_KEY (str): Tag Key
        TAG_VALUE (str): Tag Value

    Returns:

    """
    new_instance = e.create_instances(
        ImageId=IMAGE_ID,
        KeyName=KEY_NAME,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[SECURITY_GROUP],
        IamInstanceProfile={
            'Name': IAM_NAME
        },
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{
                "Key": TAG_KEY,
                "Value": TAG_VALUE
            }]
        }]
    )

    return new_instance

def authorize_ssh(e, sg_id):
    """

    Args:
        e boto3.Client: Client
        sg_id: Security Group Id

    Returns:
        None
    """

    port = 22
    myip = get_myip()
    try:
        sg = e.authorize_security_group_ingress(
            GroupId=sg_id,
            CidrIp=myip,
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port),
        )
    except ClientError as e:
        print(e)

def execute_shell_script(i_id, config, commands, sleep=2):
    """
    Make sure EC2 instance has policy AmazonSSMManagedInstanceCore
    Args:
        i_id (str): instance id
        config (cfg):
        commands (list): list of commands (str)
        sleep (int): sleep time (necessary for output)

    Returns:
        dict: output
    """
    ssm = boto3.client('ssm',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET"))

    response = ssm.send_command(
        InstanceIds=[i_id],
        DocumentName='AWS-RunShellScript',
        Parameters={"commands": commands}
    )
    import time
    time.sleep(sleep)
    command_id = response['Command']['CommandId']
    output = ssm.get_command_invocation(
        CommandId=command_id,
        InstanceId=i_id,
    )
    return output



