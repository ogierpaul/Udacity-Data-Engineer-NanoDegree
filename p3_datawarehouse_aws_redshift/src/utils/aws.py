import requests
import boto3
import pandas as pd

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

def get_cluster_properties(config):
    """
    Read the cluster properties, returns the following keys:
    ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
    "NumberOfNodes", 'VpcId', 'Endpoint_address', 'Role_arn']
    Args:
        config:

    Returns:
        pd.Series (Keys:
    """
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    region= config.get("REGION", "REGION")
    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    x = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    x = [(k, v) for k, v in x.items()]
    x = pd.DataFrame(data=x, columns=['Key', 'Value']).set_index('Key')['Value']
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    for k in keysToShow:
        try:
            assert k in x.index
        except:
            raise KeyError('Missing key {}'.format(k))
    x.loc['Endpoint_address'] = x.loc['Endpoint']['Address']
    x.loc['Role_arn'] = x.loc['IamRoles'][0]['IamRoleArn']
    return x
