import psycopg2
from dendutils.iam import get_myip
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


