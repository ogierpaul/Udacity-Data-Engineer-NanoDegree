from dendutils.config import get_project_config
from dendutils.ec2 import execute_shell_script, getOrCreate, terminate_instances
import os
import time
import logging
from dendutils.redshift import execute_statements
import psycopg2.sql as S

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')

config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/config/config_path.cfg'

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
    s3_outputfolder =  config.get("S3", "SIREN_OUTPUTFOLDER")
    ec2dir = '/home/ec2-user'

    s3_bucket = s3_bucket.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    ec2dir = ec2dir.rstrip('/')

    siren_url = config.get("DATA", "SIREN_URL")
    siren_csv=config.get("DATA", "SIREN_CSV")
    # Create params used for the commands
    params = {
        'url': siren_url,
        'ec2dir': ec2dir,
        'csvname': siren_csv,
        'input_file_ec2': ec2dir + '/siren.zip',
        'output_siren_s3': 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'siren.csv',
    }

    # Read the bash statement and format it with params
    moddir = os.path.dirname(os.path.abspath(__file__))
    bash_file = '1_siren_ec2_instructions.sh'
    mypath = os.path.join(moddir, bash_file)
    with open(mypath, 'r') as f:
        commands_all = f.read().format(**params)
    commands = commands_all.split('\n')
    commands = [c for c in commands if ((len(c) > 0) and (c[0] != '#'))]
    return commands

def transform_inside_redshift(config):
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'siren_load.sql')
    params = {
        'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
    }
    execute_statements(filepath=copy_statement, config=config, params=params)

def load_from_s3(config):
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'siren_copy.sql')
    #TODO: Replace s3 path with bucket
    p = {'table': 'staging_siren', 'inputpath': 's3://paulogiereucentral1/p6/siren/input/StockUniteLegale_utf8.csv'}
    params = {
        'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'arn': S.Literal(config.get("IAM", "CLUSTER_IAM_ARN")),
        'region': S.Literal(config.get("REGION", "REGION")),
        'table': S.Identifier(p['table']),
        'inputpath': S.Literal(p['inputpath'])
    }
    execute_statements(filepath=copy_statement, config=config, params=params)
    return None


if __name__ == '__main__':
    logger.info("Starting main")
    config = get_project_config(config_path)
    i_id = getOrCreate(config)
    commands =  read_commands(config)
    for c in commands:
        logger.info(c)
    o = execute_shell_script(InstanceId=i_id, config=config, commands=commands, sleep=180)
    terminate_instances(config)
    # load_from_s3(config)
    # transform_inside_redshift(config)





