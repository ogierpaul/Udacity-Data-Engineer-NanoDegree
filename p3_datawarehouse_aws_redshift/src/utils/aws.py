import requests
import boto3

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

def get_endpoint(config):
    """
    Endpoint of the Redshift cluster
    Args:
        config: Config file with the AWS KEY and SECRET key, as well as the DWH_CLUSTER_IDENTIFIER

    Returns:
        str: Endpoint (Example: mycluster.jlkjafljlk12kb.us-west-2.redshift.amazonaws.com
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
    v = x['Endpoint']['Address']
    return v
