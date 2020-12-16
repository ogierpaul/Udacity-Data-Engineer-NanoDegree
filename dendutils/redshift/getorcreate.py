import logging
import time
from botocore.exceptions import ClientError
import boto3
import pandas as pd

from dendutils.iam import create_iam_role, open_ports
from .redshift import get_cluster_properties, get_conn

def get_cluster_status(config):
    logger = logging.getLogger()
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_IDENTIFIER")
    region = config.get("REGION", "REGION")
    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    notfound = False
    try:
        x = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            notfound = True
        pass
    if notfound:
        return 'NotFound'
    else:
        cluster_status = x['Clusters'][0]['ClusterStatus']
        availability_status = x['Clusters'][0]['ClusterAvailabilityStatus']
        if availability_status == 'Available':
            return 'Available'
        elif availability_status == 'Paused':
            return 'Paused'
        elif cluster_status in ['deleting', 'final-snapshot']:
            return 'Deleting'
        elif availability_status in ['Maintenance', 'Modifying'] or cluster_status in ['creating', 'rebooting', 'renaming', 'restoring']\
            or (availability_status == 'Unavailable' and cluster_status == 'available'):
            return 'Modifying'
        else:
            logger.warning(f"Availability Status {availability_status} ClusterStatus {cluster_status}")
            return None


def _create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, CLUSTER_IDENTIFIER, DB_NAME,
                    DB_USER, DB_PASSWORD, DB_PORT, sleep=120, retry=None):
    """
    Create Redshift Cluster
    Args:
        redshift: Redshift boto3 client
        roleArn: ARN of DWH_IAM_ROLE
        CLUSTER_TYPE: Cluster Type (Ex: multi-node)
        NODE_TYPE: Node type (Ex: dc2.large)
        NUM_NODES: Number of nodes (Ex: 2)
        DB_NAME: Database name (Ex: mydatabase)
        CLUSTER_IDENTIFIER: (Ex: mycluster)
        DB_USER: (Ex: myuser)
        DB_PASSWORD: (Ex: mypassword)

    Returns:
        None
    """
    logger = logging.getLogger()
    try:
        print("2.1. Creating redshift cluster")
        response = redshift._create_cluster(
            # Cluster Hardware specifications
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            # Cluster Identifier
            ClusterIdentifier=CLUSTER_IDENTIFIER,

            # Database Identifiers & Credentials
            DBName=DB_NAME,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            Port=DB_PORT,

            # Roles (for s3 access) / Note that IamRoles is a list
            IamRoles=[roleArn]
        )
        logger.info(f'waiting {sleep} seconds after cluster creation')
        time.sleep(sleep)
        logger.info('resuming')
    except Exception as e:
        print(e)


def create_cluster(config, sleep=120, retry=None):
    """
    Use the admin config file
    In this order:
    Instructs AWS to:
    1. create a IAM role for the DWH
    2. create a Redshift Cluster with the associate DWH IAM Role
    3. get the cluster properties
    4. open the ports on EC2 for the DWH
    5. test connection

    Args:
        config:

    Returns:
        None
    """
    logger = logging.getLogger()
    logger.info('starting cluster creation top routine')
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    CLUSTER_TYPE = config.get("REDSHIFT", "CLUSTER_TYPE")
    NUM_NODES = config.get("REDSHIFT", "NUM_NODES")
    NODE_TYPE = config.get("REDSHIFT", "NODE_TYPE")
    CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_IDENTIFIER")

    DB_NAME = config.get("DB", "DB_NAME")
    DB_USER = config.get("DB", "DB_USER")
    DB_PASSWORD = config.get("DB", "DB_PASSWORD")
    DB_PORT = config.get("DB", "DB_PORT")
    region = config.get("REGION", "REGION")
    CLUSTER_IAM_ROLE_NAME = config.get("IAM", "CLUSTER_IAM_ROLE_NAME")
    CLUSTER_POLICY=config.get("IAM", "CLUSTER_POLICY")

    params = {
        "CLUSTER_TYPE": CLUSTER_TYPE,
        "NUM_NODES": NUM_NODES,
        "NODE_TYPE": NODE_TYPE,
        "CLUSTER_IDENTIFIER": CLUSTER_IDENTIFIER,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_PORT": DB_PORT,
        "CLUSTER_IAM_ROLE_NAME": CLUSTER_IAM_ROLE_NAME
    }
    df = pd.Series(params)
    logger.info(df)

    ec2 = boto3.client('ec2',
                         region_name=region,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name=region
                       )

    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    roleArn = create_iam_role(iam, CLUSTER_IAM_ROLE_NAME, CLUSTER_POLICY)

    _create_cluster(redshift=redshift,
                    roleArn=roleArn,
                    CLUSTER_TYPE=CLUSTER_TYPE,
                    NODE_TYPE=NODE_TYPE,
                    NUM_NODES=NUM_NODES,
                    CLUSTER_IDENTIFIER=CLUSTER_IDENTIFIER,
                    DB_NAME=DB_NAME,
                    DB_USER=DB_USER,
                    DB_PASSWORD=DB_PASSWORD,
                    DB_PORT=int(DB_PORT),
                    sleep=sleep,
                    retry=retry
                    )

    logger.info("2.2. Showing cluster properties")
    cluster_properties = get_cluster_properties(config)
    logger.info(cluster_properties.loc[["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']])
    host = cluster_properties.loc['Endpoint_address']
    logger.info(f'host:{host}')
    logger.info("2.3. Opening Port")
    open_ports(config, cluster_properties)
    logger.info("3. Testing connections")
    conn = get_conn(config)
    conn.close()
    logger.info('Connected')
    return None


def _on_paused(rsc, identifier, sleep):
    """
    On Status stop, restart
    Args:
        ecr:
        instances:
        sleep:

    Returns:

    """
    logger = logging.getLogger()
    rsc.resume_cluster(
        ClusterIdentifier=identifier
    )
    logger.info(
        f'waiting stopped cluster availability {sleep} seconds')
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
    create_cluster(config, sleep)
    return None


def _on_pending(sleep):
    """
    If instance pending, wait
    Args:
        instances:
        sleep:

    Returns:

    """
    logger = logging.getLogger()
    logger.info(f'waiting pending instances availability for {sleep} seconds')
    time.sleep(sleep)
    return None


def getOrCreate(config, retry=3, sleep=60):
    """
    Try to get, or create, a cluster matching the CLUSTER_IDENTIFIER
    Args:
        config:
        retry:
        sleep:

    Returns:
        str: Instance Id
    """

    logger = logging.getLogger()
    CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_IDENTIFIER")
    rsc = boto3.client('redshift',
                       region_name=config.get("REGION", "REGION"),
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                       )
    n = 0
    while n <= retry:
        n+=1
        logger.info(f'Getting Cluster {CLUSTER_IDENTIFIER}: Try {n}  of {retry}')
        status = get_cluster_status(config)
        logger.info(f'Cluster status: {status}')
        if status == 'Available':
            return True
        elif status == 'Paused':
            _on_paused(rsc, CLUSTER_IDENTIFIER, sleep)
        elif status == 'Modifying':
            _on_pending(sleep)
        elif status == 'NotFound':
            _on_no_instances(config, sleep)
        elif status == 'Deleting':
            time.sleep(3*sleep)
            _on_no_instances(config, sleep)

    raise ConnectionError(f'Unable to create cluster with Identifier {CLUSTER_IDENTIFIER})')