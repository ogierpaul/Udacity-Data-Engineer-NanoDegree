import time
import psycopg2
from iam import create_iam_role, open_ports
from dendutils import get_myip
import boto3
import pandas as pd
import logging
import psycopg2.sql as S
from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)

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
    CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_IDENTIFIER")
    region= config.get("REGION", "REGION")
    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    x = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    x = [(k, v) for k, v in x.items()]
    x = pd.DataFrame(data=x, columns=['Key', 'Value']).set_index('Key')['Value']
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    for k in keysToShow:
        try:
            assert k in x.index
        except:
            raise KeyError(f'Missing key {k} in index {x.index}')
    x.loc['Endpoint_address'] = x.loc['Endpoint']['Address']
    x.loc['Role_arn'] = x.loc['IamRoles'][0]['IamRoleArn']
    return x


def get_conn(config):
    """

    Args:
        config: config file, see project readme for structure

    Returns:
        psycopg2.Connector
    """
    logger = logging.getLogger()
    host = get_cluster_properties(config)['Endpoint_address']
    logger.info(f"ENDPOINT ADDRESS:{host}")
    DWH_DB = config.get("DB", "DB_NAME")
    DWH_DB_USER = config.get("DB", "DB_USER")
    DWH_DB_PASSWORD = config.get("DB", "DB_PASSWORD")
    DWH_PORT = config.get("DB", "DB_PORT")
    conn_info = "host={} dbname={} user={} password={} port={}".format(
        host,
        DWH_DB,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        DWH_PORT
    )
    ip = get_myip()
    logger.info(f'active ip:{ip}')
    conn = psycopg2.connect(conn_info)
    conn.autocommit = True
    logger.info(f'connection status :{conn.status}')
    active = (conn.closed == 0)
    logger.info(f"connection active: {active}")
    return conn

def create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, CLUSTER_IDENTIFIER, DB_NAME,
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
    #TODO: Add test if cluster exists, retry
    try:
        print("2.1. Creating redshift cluster")
        response = redshift.create_cluster(
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

def create_cluster_main(config, sleep=120, retry=None):
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

    create_cluster(redshift=redshift,
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

def execute_statements(filepath, config, params):
    logger = logging.getLogger()
    logger.info(f"reading file {filepath}")
    statements = open(filepath,'r').read()
    sql_all = [S.SQL(q.strip() + ';').format(**params) for q in statements.split(';') if q.strip() != '']
    conn= get_conn(config)
    cur = conn.cursor()
    for q in sql_all:
        logger.info(f"execute query:{q.as_string(conn)}")
        try:
            cur.execute(q)
        except (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError) as e:
            print(e)
    logger.info(f"finished executing file {filepath}")
    conn.close()
    return True


