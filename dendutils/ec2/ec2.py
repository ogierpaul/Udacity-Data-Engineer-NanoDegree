import boto3
import logging
from botocore.exceptions import ClientError

def show_all_instances_status(e):
    """

    Args:
        e (boto3.client):

    Returns:
        list of tuples (instance_id, state_name)
    """
    o = [
        (c['Instances'][0]['InstanceId'], c['Instances'][0]['State']['Name']) \
        for c in e.describe_instances()['Reservations']
    ]
    logger = logging.getLogger()
    logger.info(f"SHOWING instances:\n{o}")
    return o


def filter_per_tag(e, key, value):
    """
    Filter instance per tag
    Args:
        e: boto3 ec2 client
        key (str): Tag key
        value (str): Tag Value

    Returns:
        list: list of strings , instances ids
    """

    query = [{
        "Name": "tag-key",
        "Values": [key]
    }]
    res = []
    props = e.describe_instances(Filters=query)['Reservations']
    for i in props:
        j = i['Instances']
        j_id = j[0]['InstanceId']
        for t in j[0]['Tags']:
            if t.get("Key") == key and t.get("Value") == value:
                res.append(j_id)
    return res


def create_vm(config,sleep=15, retry=3):
    logger = logging.getLogger()
    ec2r = boto3.resource('ec2',
                         region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")
                  )
    ec2c = boto3.client('ec2',
                         region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")
                  )
    _create_vm(
        ec2r,
    )
    try:
        i = _create_vm(
            ec2r,
            config.get("EC2", "IMAGE_ID"),
            config.get("EC2", "KEY_PAIR"),
            config.get("EC2", "INSTANCE_TYPE"),
            config.get("SG", "SG_ID"),
            config.get("IAM", "IAM_EC2_ROLE"),
            config.get("EC2", "TAG_KEY"),
            config.get("EC2", "TAG_VALUE")
        )
        o = show_all_instances_status(ec2c)
        logger.info(o)
    except ClientError as e:
        print(e)

    return i



def _create_vm(e, IMAGE_ID, KEY_NAME, INSTANCE_TYPE, SECURITY_GROUP, IAM_NAME, TAG_KEY, TAG_VALUE, sleep=15, retry=3):
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
        sleep (int): time to sleep between retries
        retry (int): Number of retries

    Returns:

    """
    logger = logging.getLogger()
    logger.info("CREATING INSTANCE")
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
    logger.info(f"INSTANCE CREATED WITH ID {new_instance[0].id}")
    return new_instance


def execute_shell_script(i_id, config, commands, sleep=2):
    """
    Make sure EC2 instance has policy AmazonSSMManagedInstanceCore
    See doc here: https://docs.aws.amazon.com/cli/latest/reference/ssm/get-command-invocation.html
    Args:
        i_id (str): instance id
        config (cfg): config file with AWS credentials ("AWS", "KEY") and ("AWS", SECRET") and ("REGION", "REGION")
        commands (list): list of commands (str)
        sleep (int): sleep time (necessary for output)

    Returns:
        dict: output, key for output is StandardOutputContent
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
    # TODO: Add try and retry after sleep / get command invocation directly / use yield??
    command_id = response['Command']['CommandId']
    output = ssm.get_command_invocation(
        CommandId=command_id,
        InstanceId=i_id,
    )
    return output
