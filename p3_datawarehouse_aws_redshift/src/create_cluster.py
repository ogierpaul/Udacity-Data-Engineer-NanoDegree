import pandas as pd
import psycopg2
import boto3
import json


# Inspired from https://github.com/Flor91/Data-engineering-nanodegree/blob/master/2_dend_cloud_data_warehouses/P3_Data_Warehouse_Project/create_cluster.py
## Changes / Addition
# - Rewrote script using my style to better understand the steps
# - Updated the doc strings
# - added condition to only open ports on myIP
# - the main routine needs a config parameter

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Create an IAM role for the Redshift Cluster (The Principal), to allow AmazonS3ReadOnlyAccess (the policy)
    Args:
        iam (bot3.client): boto3 IAM Object
        DWH_IAM_ROLE_NAME (str): name to be given to the IAM role

    Returns:
        roleIARN
    """

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn


def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER,
                   DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    Creates Redshift cluster
    '''

    try:
        print("2.1. Creating redshift cluster")
        response = redshift.create_cluster(
            # Cluster Hardware specifications
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Cluster Identifier
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,

            # Database Identifiers & Credentials
            DBName=DWH_DB,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access) / Note that IamRoles is a list
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)


def get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER):
    """

    Args:
        redshift (bot3.client): Redshift Client
        DWH_CLUSTER_IDENTIFIER: Cluster Id

    Returns:
        pd.DataFrame: Properties of the cluster
    """
    print("2.2. Showing cluster properties")
    x = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    x = [(k, v) for k, v in x.items()]
    x = pd.DataFrame(data=x, columns=['Key', 'Value']).set_index('Key')['Value']

    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    print(x.loc[keysToShow])
    x.loc['DWH_ENDPOINT'] = x.loc['Endpoint']['Address']
    x.loc['ROLE_ARN'] = x.loc['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", x.loc['DWH_ENDPOINT'])
    print("DWH_ROLE_ARN :: ", x.loc['ROLE_ARN'])
    return x


def open_ports(ec2, cluster_properties, DWH_PORT):
    '''
    Update clusters security group to allow access through redshift port
    '''
    print("2.3 Opening port of the cluster")
    def get_myip():
        import requests
        r = requests.get('http://checkip.amazonaws.com/')
        r = r.text.rstrip('\n')
        r += '/32'
        return r
    myip = get_myip()
    print('Cidr IP block of executable:', myip)
    try:
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=myip,
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def main(config):
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

    DWH_DB = config.get("DB", "DB_NAME")
    DWH_DB_USER = config.get("DB", "DB_USER")
    DWH_DB_PASSWORD = config.get("DB", "DB_PASSWORD")
    DWH_PORT = config.get("DB", "DB_PORT")

    DWH_IAM_ROLE_NAME = config.get("IAM", "DWH_IAM_ROLE_NAME")

    params = {
        "DWH_CLUSTER_TYPE": DWH_CLUSTER_TYPE,
        "DWH_NUM_NODES": DWH_NUM_NODES,
        "DWH_NODE_TYPE": DWH_NODE_TYPE,
        "DWH_CLUSTER_IDENTIFIER": DWH_CLUSTER_IDENTIFIER,
        "DWH_DB": DWH_DB,
        "DWH_DB_USER": DWH_DB_USER,
        "DWH_DB_PASSWORD": DWH_DB_PASSWORD,
        "DWH_PORT": DWH_PORT,
        "DWH_IAM_ROLE_NAME": DWH_IAM_ROLE_NAME
    }
    df = pd.Series(params)
    print(df)

    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_cluster(redshift,
                   roleArn,
                   DWH_CLUSTER_TYPE,
                   DWH_NODE_TYPE,
                   DWH_NUM_NODES,
                   DWH_DB,
                   DWH_CLUSTER_IDENTIFIER,
                   DWH_DB_USER,
                   DWH_DB_PASSWORD
                   )

    cluster_properties = get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)

    open_ports(ec2, cluster_properties, DWH_PORT)
    host = cluster_properties.loc['DWH_ENDPOINT']
    print('host:', host)
    print("3. Testing connections")
    conn=psycopg2.connect(
            dbname=DWH_DB,
            host=host,
            port=DWH_PORT,
            user=DWH_DB_USER,
            password=DWH_DB_PASSWORD
    )
    print(conn)
    cur = conn.cursor()
    cur.execute("""select 1 as foo""")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    print('Connected')
    conn.close()
    return None




