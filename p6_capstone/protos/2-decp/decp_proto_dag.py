from dendutils.config import get_project_config
from dendutils.ec2 import execute_shell_script_config,  terminate_instances_config
from dendutils.redshift import execute_statements
from dendutils.ec2 import  getOrCreate_config as ec2_getOrCreate
from dendutils.redshift import getOrCreate as rs_getOrCreate
from s3 import upload_file_config
from botocore.exceptions import ClientError
import os
import time
import logging
import psycopg2.sql as S


import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')

config_path = '/config/config_path.cfg'

def upload_jq_statements(config):
    """
    Copy the jq statements from this module to s3 for import into ec2
    Args:
        config: paths [S3, BUCKET], [S3, DECP_CONFIGFOLDER]

    Returns:

    """
    logger.info('starting upload of jq statements to s3')
    #Moddir is the module directory
    moddir = os.path.dirname(os.path.abspath(__file__))
    s3_bucket = config.get('S3', 'BUCKET')
    s3_folder = config.get('S3', 'DECP_CONFIGFOLDER')
    jq_marches = os.path.join(moddir, 'jq_marches.sh')
    jq_titulaires = os.path.join(moddir, 'jq_titulaires.sh')
    for f in [jq_marches, jq_titulaires]:
        object_name = s3_folder.rstrip('/') + '/' + f.split('/')[-1]
        upload_file_config(config, bucket=s3_bucket, file_path=f, object_name=object_name)
        logger.info(f'end download of {f}')
    return None


def read_commands(config):
    """
    Read decp_ec2_instructions.sh and format it with params from the config file
    Args:
        config:

    Returns:
        list: list of str, each str is a bash command
    """
    # Read params from config file (and clean directories names)
    s3_bucket = config.get("S3", "BUCKET")
    s3_configfolder =  config.get("S3", "DECP_CONFIGFOLDER")
    s3_outputfolder =  config.get("S3", "DECP_OUTPUTFOLDER")
    ec2dir = '/home/ec2-user'

    s3_bucket = s3_bucket.rstrip('/')
    s3_configfolder = s3_configfolder.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    ec2dir = ec2dir.rstrip('/')

    decp_url = config.get("DATA", "DECP_URL")

    # Create params used for the commands
    params = {
        'jq_marches_s3': 's3://' + s3_bucket + '/' + s3_configfolder + '/jq_marches.sh',
        'jq_titulaires_s3': 's3://' + s3_bucket + '/' + s3_configfolder + '/jq_titulaires.sh',
        'url': decp_url,
        'ec2dir': ec2dir,
        'jq_marches_ec2': ec2dir + '/jq_marches.sh',
        'jq_titulaires_ec2': ec2dir + '/jq_titulaires.sh',
        'input_file_ec2': ec2dir + '/decp.json',
        'output_temp_ec2': ec2dir + '/outputtemp.json',
        'output_marches_ec2': ec2dir + '/marches.json',
        'output_titulaires_ec2': ec2dir + '/titulaires.json',
        'output_titulaires_s3': 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'titulaires.json',
        'output_marches_s3': 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'marches.json',
    }

    # Read the bash statement and format it with params
    moddir = os.path.dirname(os.path.abspath(__file__))
    bash_file = '1_decp_ec2_instructions.sh'
    mypath = os.path.join(moddir, bash_file)
    with open(mypath, 'r') as f:
        commands_all = f.read().format(**params)
    commands = commands_all.split('\n')
    commands = [c for c in commands if ((len(c) > 0) and (c[0] != '#'))]
    return commands

def copy_from_s3(config):
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), '2_decp_copy_s3.sql')
    s3_bucket = config.get("S3", "BUCKET")
    s3_outputfolder =  config.get("S3", "DECP_OUTPUTFOLDER")
    s3_bucket = s3_bucket.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    output_titulaires_s3 = 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'titulaires.json'
    output_marches_s3 =  's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'marches.json'
    d = [
        {'table': 'staging_decp_marches', 'inputpath': output_marches_s3},
        {'table': 'staging_decp_titulaires', 'inputpath': output_titulaires_s3}
    ]
    for p in d:
        params = {
            'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
            'arn': S.Literal(config.get("IAM", "CLUSTER_IAM_ARN")),
            'region': S.Literal(config.get("REGION", "REGION")),
            'table': S.Identifier(p['table']),
            'inputpath': S.Literal(p['inputpath'])
        }
        execute_statements(filepath=copy_statement, config=config, params=params)
    return None

def stage_inside_redshift(config):
    # TODO: Rewrite "myext".decp_marches column "datenotification" is of type date but expression is of type character varying
    execute_statements(
        filepath=os.path.join(os.path.dirname(os.path.abspath(__file__)), '3_decp_redshift_stage.sql'),
        config=config,
        params={
            'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
            'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
        }
    )



if __name__ == '__main__':
    logger.info("Starting main")
    config = get_project_config(config_path)
    i_id = ec2_getOrCreate(config)
    upload_jq_statements(config)
    commands =  read_commands(config)
    o = execute_shell_script_config(InstanceId=i_id, config=config, commands=commands, sleep=180)
    terminate_instances_config(config)
    rs = rs_getOrCreate(config)
    copy_from_s3(config)
    stage_inside_redshift(config)
    print('done')





