import logging

import boto3


def get_instance_status(ecc, Instanceid):
    """
    Return a synthetized instance status: 'available', 'stopped', 'deleting', 'modifying', None
    Used to simplify the instance check before launching any queries
    Args:
        ecc (boto3.Client): boto3 ec2 client
        Instanceid (str): Instance number

    Returns:

    """
    logger = logging.getLogger()
    assert isinstance(Instanceid, str)
    out = ecc.describe_instance_status(InstanceIds=[Instanceid], IncludeAllInstances=True)
    props = out['InstanceStatuses'][0]
    instance_status = props['InstanceStatus']['Status']
    system_status = props['SystemStatus']['Status']
    instance_state = props['InstanceState']['Name']
    if system_status == 'ok' and instance_status == 'ok' and instance_state == 'running':
        return 'available'
    elif instance_state in ['stopping', 'stopped']:
        return 'stopped'
    elif instance_state in ['shutting-down', 'terminated']:
        return 'deleting'
    elif instance_state in ['pending', 'resizing'] \
        or system_status in ['initializing'] \
        or instance_status in ['initializing']:
        return 'modifying'
    else:
        logger.warning(f"Instance State {instance_state} System Status {system_status}")
        return None


def show_instances_status(config):
    """
    Show instances status matching the config file TAG_KEY, TAG_VALUE filter
    :param config:
    :return: list of
    """
    query = [{
        "Name": f"tag:{config.get('EC2', 'TAG_KEY')}",
        "Values": [config.get("EC2", "TAG_VALUE")]
    }]
    ecc = boto3.client('ec2',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    o = [
        (c['Instances'][0]['InstanceId'], c['Instances'][0]['State']['Name']) \
        for c in ecc.describe_instances(Filters=query)['Reservations']
    ]
    return o


def _show_all_instances_status(e):
    """
    Show state from all instances
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