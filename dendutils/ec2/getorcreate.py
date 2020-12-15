import logging
import time

import boto3
from botocore.exceptions import ClientError

from .status import  get_instance_status
from .find import filter_per_tag


def create_vm(config):
    """
    Create an EC2 Instance from the parameters defined in the EC2 section:
    IMAGE_ID, KEY_PAIR, INSTANCE_TYPE, TAG_KEY, TAG_VALUE
    and the security Group SG:SG_ID and IAM:IAM_EC2_ROLE
    Args:
        config:

    Returns:
        boto3.instance
    """
    logger = logging.getLogger()
    ec2r = boto3.resource('ec2',
                          region_name=config.get("REGION", "REGION"),
                          aws_access_key_id=config.get("AWS", "KEY"),
                          aws_secret_access_key=config.get("AWS", "SECRET")
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
    except ClientError as e:
        logger.warning(e)
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
        boto3.instance
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


def _on_stopped(ecr, instances, sleep):
    """
    On Status stop, restart
    Args:
        ecr:
        instances:
        sleep:

    Returns:

    """
    logger = logging.getLogger()
    i_id = instances[0][0]
    ecr.instances.filter(InstanceIds=[i_id]).start()
    logger.info(
        f'waiting stopped instances availability {i_id} for {sleep} seconds')
    time.sleep(sleep)
    return None

def _on_no_instances(config, sleep):
    """
    If no instances, create
    Args:
        config:
        sleep:

    Returns:

    """
    logger = logging.getLogger()
    logger.info(f'creating instance')
    i_id = create_vm(config)
    logger.info(f'waiting newly created instances {i_id} availability for {sleep} seconds')
    time.sleep(sleep)
    return None


def _on_pending(instances, sleep):
    """
    If instance pending, wait
    Args:
        instances:
        sleep:

    Returns:

    """
    logger = logging.getLogger()
    logger.info(
        f'waiting pending instances availability {[c[0] for c in instances]} for {sleep} seconds')
    time.sleep(sleep)
    return None


def getOrCreate(config, retry=3, sleep=30):
    """
    Try to get, or create, an instance matching the TAG_KEY, TAG_VALUE
    Args:
        config:
        retry:
        sleep:

    Returns:
        str: Instance Id
    """

    logger = logging.getLogger()
    TAG_KEY = config.get("EC2", "TAG_KEY")
    TAG_VALUE = config.get("EC2", "TAG_VALUE")
    ecc = boto3.client('ec2',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    ecr = boto3.resource('ec2',
                         region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")
                         )
    n = 0
    res_id = None
    while n <= retry and res_id is None:
        instances = filter_per_tag(ecc, TAG_KEY, TAG_VALUE)
        # Get the list of active instances
        if instances is None or len(instances) == 0:
            logger.info(f"No instances found")
            available_instances = stopped_instances = pending_instances = []
        else:
            instances_status = [(c_id, get_instance_status(ecc, c_id)) for c_id in instances]
            logger.info(f"instances per status:\n{instances_status}")
            available_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'available']
            stopped_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'stopped']
            pending_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'modifying']
        if len(available_instances) > 0:
            res_id = available_instances[0][0]
        elif len(stopped_instances) > 0:
            _on_stopped(ecr, stopped_instances, sleep)
        elif len(pending_instances) > 0:
            _on_pending(pending_instances, sleep)
        else:
            _on_no_instances(config, sleep)

    if res_id is None:
        raise ClientError(f'Unable to create instance with tag ({TAG_KEY}, {TAG_VALUE})')
    else:
        logger.info(f'active instance_id:{res_id}')
        return res_id


