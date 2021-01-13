import boto3

from .status import get_instance_status


# ForLater: Define one filter_per_tag, One filter_per_status with same syntax

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
        "Name": f"tag:{key}",
        "Values": [value]
    }]
    props = e.describe_instances(Filters=query)['Reservations']
    if len(props) > 0:
        res = [i['Instances'][0]['InstanceId'] for i in props]
    else:
        res = []
    return res


def _check_is_instanceid(i):
    assert isinstance(i, str)
    assert i[0] == 'i'
    assert len(i) > 4
    return True


def filter_on_custom_states_config(config, states):
    """
    Filter on custom states the VMs matching the TAG_KEY, TAG_VALUE parameters from config
    :param config:
    :param states (list): must be one of ['available', 'modifying', 'stopped', 'deleting']
    :return: list of instance ids
    """
    assert not isinstance(states, str)
    assert isinstance(states, list)
    for c in states:
        assert c in ['available', 'modifying', 'stopped', 'deleting']
    ecc = boto3.client('ec2',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    instances = filter_per_tag(ecc, config.get('EC2', 'TAG_KEY'), config.get("EC2", "TAG_VALUE"))
    instances_status = [(c_id, get_instance_status(ecc, c_id)) for c_id in instances]
    target_instances = [c_id for c_id, c_stat in instances_status if c_stat in states]
    return target_instances
