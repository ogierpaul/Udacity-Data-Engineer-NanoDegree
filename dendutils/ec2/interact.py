import time

import boto3
import logging

from .find import filter_on_custom_states_config
from .status import get_instance_status


def execute_shell_commands(region_name, aws_access_key_id, aws_secret_access_key, InstanceId, commands, sleep=3,
                           retry=10):
    """
    Execute list of shell commands with on Ec2 Instance
    Make sure EC2 instance has policy AmazonSSMManagedInstanceCore
    See doc here: https://docs.aws.amazon.com/cli/latest/reference/ssm/get-command-invocation.html
    Args:
        region_name (str): AWs Region Name
        aws_access_key_id (str):
        aws_secret_access_key (str):
        InstanceId (str): Id of EC2 Instance
        commands (list): list of str bash commands
        retry (int): number of time to try to get command status until available
        sleep (int): time to wait between each try


    Returns:
        dict: output, key for output is StandardOutputContent

    Description:
    1. Initialize SSM client
    2. Use SSM to launch the list of commands to the InstanceId
    3. Start loop: try to get status as Success. Between each try, wait (sleep) seconds.\
        Loop is broken if max number of try has been reached, or if status is in Success or Failed.
    4. Return output
    """
    logger = logging.getLogger()
    ssm = boto3.client('ssm',
                       region_name=region_name,
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key
                       )
    logger.info('starting execution')
    for q in commands:
        logger.info(q)
    response = ssm.send_command(
        InstanceIds=[InstanceId],
        DocumentName='AWS-RunShellScript',
        Parameters={"commands": commands}
    )
    command_id = response['Command']['CommandId']
    logger.info(f"command id {command_id} InstanceId {InstanceId}")
    time.sleep(2)
    n = 0
    finished = False
    output = None
    status = "Waiting Status"
    while n < retry and finished is False:
        n += 1
        logger.info(f"Try {n} of {retry}: getting status...")
        output = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=InstanceId
        )
        status = output['Status']
        logger.info(f'Command status {status}')
        if status == 'Success':
            finished = True
            break
        elif status in ['Pending', 'Delayed', 'InProgress', 'Cancelling']:
            finished = False
            time.sleep(sleep)
            logger.info(f"sleep {sleep}")
        elif status in ['Cancelled', 'TimedOut', 'Failed']:
            finished = True
            break
        else:
            pass
    logger.info(
        f"InstanceId {InstanceId} Command Id {command_id}:\nFinal status {status}\nOutput {output['StandardOutputContent']}")
    return output


def execute_shell_script_config(InstanceId, config, commands, sleep=2, retry=8):
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
    output = execute_shell_commands(
        region_name=config.get("REGION", "REGION"),
        aws_access_key_id=config.get("AWS", "KEY"),
        aws_secret_access_key=config.get("AWS", "SECRET"),
        InstanceId=InstanceId,
        commands=commands,
        sleep=sleep,
        retry=retry

    )

    return output


def terminate_instances(region_name, aws_access_key_id, aws_secret_access_key, InstanceIds, retry=3, sleep=10):
    logger = logging.getLogger()
    ecr = boto3.resource('ec2',
                         region_name=region_name,
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key
                         )
    ecc = boto3.client('ec2',
                       region_name=region_name,
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key
                       )
    n = 0

    while n <= retry:
        n += 1
        assert isinstance(InstanceIds, list)
        instances_status = [(c_id, get_instance_status(ecc, c_id)) for c_id in InstanceIds]
        active_instances = [c_id for c_id, c_stat in instances_status if c_stat in ['available', 'modifying']]
        if len(active_instances) > 0:
            m = ecr.instances.filter(InstanceIds=active_instances).terminate()
            logger.info(m)
            time.sleep(sleep)
        else:
            break
    return None


def terminate_instances_config(config, sleep=10, retry=3):
    """
    Terminate the instances with matching the config file EC2 [Tag_Key, Tag_Value] Filter
    :param config: config file
    :return:
    """
    region_name = config.get("REGION", "REGION"),
    aws_access_key_id = config.get("AWS", "KEY"),
    aws_secret_access_key = config.get("AWS", "SECRET")
    target_instances = filter_on_custom_states_config(config, states=['available', 'stopped', 'modifying'])
    terminate_instances(region_name=region_name,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        InstanceIds=target_instances,
                        retry=retry,
                        sleep=sleep
                        )
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
        target_instances = filter_on_custom_states_config(config, states=['available', 'modifying'])
        if len(target_instances) > 0:
            m = er.instances.filter(InstanceIds=target_instances).stop()
            logger.info(m)
            time.sleep(sleep)
        else:
            no_targets = True
    return None


def read_format_shell_script(fp, **kwargs):
    """
    Read a shell script template, and return it as a list of statements.
    Shell script should be one_line
    Args:
        fp (str): file path
        **kwargs: parameters to format filepath

    Returns:
        list
    """
    # Create a list of shell commands from the template file
    # Command list should be one-liners
    f = open(fp, 'r')
    commands_unformatted = f.read()
    f.close()
    commands_formatted = commands_unformatted.format(**kwargs)
    commands_list_raw = commands_formatted.split('\n')
    commands_list = list(
        filter(
            lambda c: all([len(c) > 0, c[0] != '#']),
            map(lambda c: c.strip(), commands_list_raw)
        )
    )
    return commands_list
