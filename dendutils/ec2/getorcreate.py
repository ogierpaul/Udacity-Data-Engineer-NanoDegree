import logging
import time

import boto3
from botocore.exceptions import ClientError

from .status import get_instance_status
from .find import filter_per_tag


def create_vm_config(config):
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


def _create_vm(ecr, ImageId, KeyName, InstanceType, SecurityGroupId, IamInstanceProfileName, TAG_KEY, TAG_VALUE):
    """
    See here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Subnet.create_instances
    Args:
        ecr (boto3.resource): boto3 resource
        ImageId (str): VM (AMI) image Id
        KeyName (str): PEM key name
        InstanceType (str): instance type , ex t2.micro
        SecurityGroupId (str): Security Group Name
        IamInstanceProfileName (str): Iam Role Name
        TAG_KEY (str): Tag Key
        TAG_VALUE (str): Tag Value
    Returns:
        boto3.instance
    """
    logger = logging.getLogger()
    logger.info("CREATING INSTANCE")
    new_instance = ecr.create_instances(
        ImageId=ImageId,
        KeyName=KeyName,
        InstanceType=InstanceType,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[SecurityGroupId],
        IamInstanceProfile={
            'Name': IamInstanceProfileName
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



def _on_no_instances(ecr, ImageId, KeyName, InstanceType, SecurityGroupId, IamInstanceProfileName, TAG_KEY, TAG_VALUE, StartSleep=120):
    """
    If no instance available, then create one
    Args:
        ecr: boto3 ec2 ressource
        ImageId (str):
        KeyName (str): Key-Pair name
        InstanceType (str): InstanceType
        SecurityGroupId (str): Security Group to be assigned
        IamInstanceProfileName (str): IAM Arn Role name
        TAG_KEY (str):  Tag Key
        TAG_VALUE (str): Tag Value
        StartSleep (int): Time to wait until VM should be available

    Returns:
        str: instance Id
    """
    logger = logging.getLogger()
    logger.info(f'creating instance')
    i_id = _create_vm(ecr=ecr,
                      ImageId=ImageId,
                      KeyName=KeyName,
                      InstanceType=InstanceType,
                      SecurityGroupId=SecurityGroupId,
                      IamInstanceProfileName=IamInstanceProfileName,
                      TAG_KEY=TAG_KEY,
                      TAG_VALUE=TAG_VALUE
                      )[0].id
    logger.info(f'waiting newly created instances {i_id} availability for {sleep} seconds')
    time.sleep(StartSleep)
    return i_id


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


def getOrCreate(aws_access_key_id, aws_secret_access_key, region_name, \
                ImageId, InstanceType, KeyName, IamInstanceProfileName, SecurityGroupId, \
                tag_key, tag_value, LoopSleep=30, StartSleep=60, retry=8):
    """
    Get InstanceId of Ec2 Instance matching tag_key and tag_value. If none exists, then create one.
    The method will:
    1. Initialize the arguments
    2. Loop (retry) times and at each loop, check status of instances matching tag_key and tag_value. Break the loop if one is available
    3. For each case possible (Available, Stopped, Pending, No Instance), do actions:
        Available --> break loop
        Stopped --> Resume Instance
        Pending --> Wait (sleep until it is picked as available in the next loop)
        No Instance --> Create one
    4. Return either Available InstanceId, or raise Connection Error

    Args:
        aws_access_key_id:
        aws_secret_access_key:
        region_name:
        ImageId:
        InstanceType:
        KeyName:
        IamInstanceProfileName:
        SecurityGroupId:
        tag_key:
        tag_value:
        LoopSleep: time to wait between to loop
        StartSleep: time to wait after instance has been created
        retry:

    Returns:
        str: instance id
    """
    # 1. Initialization of loops
    logger = logging.getLogger()
    ecc = boto3.client('ec2',
                       region_name=region_name,
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key
                       )
    ecr = boto3.resource('ec2',
                         region_name=region_name,
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key
                         )
    n = 0
    res_id = None
    # Loop (retry) times while waiting (sleep) secondes between each loop
    # Loop until either max number of tries has been done, or an available instance has been found
    while n <= retry and res_id is None:
        n += 1
        # 2. Get the list of instances matching the tag_key and tag_value
        instances = filter_per_tag(ecc, tag_key, tag_value)
        # 2.1. Case none exists
        if instances is None or len(instances) == 0:
            logger.info(f"No instances found")
            available_instances = stopped_instances = pending_instances = []
        # 2.2. Case some exist, find available, stopped, or pending instances
        else:
            instances_status = [(c_id, get_instance_status(ecc, c_id)) for c_id in instances]
            logger.info(f"instances per status:\n{instances_status}")
            available_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'available']
            stopped_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'stopped']
            pending_instances = [(c_id, c_stat) for c_id, c_stat in instances_status if c_stat == 'modifying']

        # 3. Decide what to do base on status of matching instances
        # 3.1. If there are some available instances>: get their id (this will stop the loop)
        if len(available_instances) > 0:
            res_id = available_instances[0][0]
        # 3.2. If there is a stopped instance, restart it, and wait (sleep) seconds, this will become available in next loop
        elif len(stopped_instances) > 0:
            _on_stopped(ecr, stopped_instances, StartSleep)
        # 3.3. If there is a pending (rebooting, modifying...) instance: wait (sleep) secondes
        elif len(pending_instances) > 0:
            _on_pending(pending_instances, LoopSleep)
        # 3.4. Last case. If there is no available, stopped or pending instance: start one.
        else:
            res_id = _on_no_instances(
                ecr=ecr,
                ImageId=ImageId,
                KeyName=KeyName,
                InstanceType=InstanceType,
                SecurityGroupId=SecurityGroupId,
                IamInstanceProfileName=IamInstanceProfileName,
                TAG_KEY=tag_key,
                TAG_VALUE=tag_value,
                StartSleep=StartSleep
            )

    # 4.: After either all the loops have been run, or an available instance has been found and its id filled:
    if res_id is None:
        raise ConnectionError(f'Unable to create instance with tag ({tag_key}, {tag_value})')
    else:
        logger.info(f'active instance_id:{res_id}')
        return res_id


def getOrCreate_config(config, retry=3, sleep=60):
    """
    Try to get, or create, an instance matching the TAG_KEY, TAG_VALUE
    Args:
        config:
        retry:
        sleep:

    Returns:
        str: Instance Id
    """

    res_id = getOrCreate(region_name=config.get("REGION", "REGION"),
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET"),
                         tag_key=config.get("EC2", "TAG_KEY"),
                         tag_value=config.get("EC2", "TAG_VALUE"),
                         ImageId=config.get("EC2", "IMAGE_ID"),
                         KeyName=config.get("EC2", "KEY_PAIR"),
                         InstanceType=config.get("EC2", "INSTANCE_TYPE"),
                         SecurityGroupId=config.get("SG", "SG_ID"),
                         IamInstanceProfileName=config.get("IAM", "IAM_EC2_ROLE"),
                         retry=retry,
                         StartSleep=sleep
                         )
    return res_id
