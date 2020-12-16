import time

import boto3
import logging

from .find import filter_on_custom_states


def execute_shell_script(InstanceId, config, commands, sleep=2, n_retry=3):
    """
    Make sure EC2 instance has policy AmazonSSMManagedInstanceCore
    See doc here: https://docs.aws.amazon.com/cli/latest/reference/ssm/get-command-invocation.html
    Args:
        InstanceId (str): instance id
        config (cfg): config file with AWS credentials ("AWS", "KEY") and ("AWS", SECRET") and ("REGION", "REGION")
        commands (list): list of commands (str)
        sleep (int): sleep time (necessary for output)

    Returns:
        dict: output, key for output is StandardOutputContent
    """
    logger = logging.getLogger()
    ssm = boto3.client('ssm',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET"))

    logger.info('commands to execute')
    for q in commands:
        logger.info(q)
    logger.info('starting execution')
    response = ssm.send_command(
        InstanceIds=[InstanceId],
        DocumentName='AWS-RunShellScript',
        Parameters={"commands": commands}
    )
    command_id = response['Command']['CommandId']
    logger.info(f"command id {command_id} InstanceId {InstanceId}")
    logger.info(f'sleep {sleep}')
    time.sleep(sleep)
    n = 0
    finished = False
    while n < n_retry and finished is False:
        n+=1
        logger.info(f"Try {n} of {n_retry}: getting status...")
        output = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=InstanceId
        )
        status = output['Status']
        logger.info(f'Command status {status}')
        if status == 'Success':
            finished = True
        elif status in ['Pending', 'Delayed', 'InProgress', 'Cancelling']:
            finished = False
            time.sleep(sleep)
            logger.info(f"sleep {sleep}")
        elif status in ['Cancelled', 'TimedOut', 'Failed' ]:
            finished = True
        else:
            pass

    return output


def terminate_instances(config, sleep=10, n_tries=3):
    """
    Terminate the instances with matching the config file EC2 [Tag_Key, Tag_Value] Filter
    :param config: config file
    :return:
    """
    ecc = boto3.client('ec2',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    er = boto3.resource('ec2',
                        region_name=config.get("REGION", "REGION"),
                        aws_access_key_id=config.get("AWS", "KEY"),
                        aws_secret_access_key=config.get("AWS", "SECRET")
                        )
    logger = logging.getLogger()
    n = 0
    no_targets = False
    while n < n_tries and no_targets is False:
        target_instances = filter_on_custom_states(config, states=['available', 'stopped', 'modifying'])
        if len(target_instances) > 0:
            m = er.instances.filter(InstanceIds=target_instances).terminate()
            logger.info(m)
            time.sleep(sleep)
        else:
            no_targets = True
    return None


def stop_instances(config, sleep=10, n_tries=3):
    """
    Terminate the instances with matching the config file EC2 [Tag_Key, Tag_Value] Filter
    :param config: config file
    :return:
    """
    ecc = boto3.client('ec2',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    er = boto3.resource('ec2',
                        region_name=config.get("REGION", "REGION"),
                        aws_access_key_id=config.get("AWS", "KEY"),
                        aws_secret_access_key=config.get("AWS", "SECRET")
                        )
    query = [{
        "Name": f"tag:{config.get('EC2', 'TAG_KEY')}",
        "Values": [config.get("EC2", "TAG_VALUE")]
    }]
    logger = logging.getLogger()
    n = 0
    no_targets = False
    while n < n_tries and no_targets is False:
        target_instances = filter_on_custom_states(config, states=['available', 'modifying'])
        if len(target_instances) > 0:
            m = er.instances.filter(InstanceIds=target_instances).stop()
            logger.info(m)
            time.sleep(sleep)
        else:
            no_targets = True
    return None
