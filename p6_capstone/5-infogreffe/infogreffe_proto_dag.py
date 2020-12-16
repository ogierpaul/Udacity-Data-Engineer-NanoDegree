from dendutils.config import get_project_config
from dendutils.ec2 import execute_shell_script, terminate_instances
from dendutils.ec2 import getOrCreate as ec2_getOrCreate
from dendutils.redshift import getOrCreate as rs_getOrCreate
import os
import time
import logging
from dendutils.redshift import execute_statements
import psycopg2.sql as S
import json

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
    s3_outputfolder = config.get("S3", "INFOGREFFE_OUTPUTFOLDER")
    ec2dir = '/home/ec2-user'

    s3_bucket = s3_bucket.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    ec2dir = ec2dir.rstrip('/')

    infogreffe_url = config.get("DATA", "INFOGREFFE_URL")
    infogreffe_csv = config.get("DATA", "INFOGREFFE_CSV")
    querypath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '1_infogreffe_query.json'
    )
    with open(querypath, 'r') as f:
        query_json = json.load(f)
    query_url = infogreffe_url + '?' + "&".join([f"{k}={query_json[k]}" for k in query_json])
    # Create params used for the commands
    params = {
        'url': query_url,
        'ec2dir': ec2dir,
        'fname_ec2': ec2dir + '/' + infogreffe_csv,
        'fname_s3': 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + infogreffe_csv,
    }

    # Read the bash statement and format it with params
    moddir = os.path.dirname(os.path.abspath(__file__))
    bash_file = '1_infogreffe_ec2_instructions.sh'
    mypath = os.path.join(moddir, bash_file)
    with open(mypath, 'r') as f:
        commands_all = f.read().format(**params)
    commands = commands_all.split('\n')
    commands = [c for c in commands if ((len(c) > 0) and (c[0] != '#'))]
    return commands



if __name__ == '__main__':
    config = get_project_config(config_path)
    i_id = ec2_getOrCreate(config)
    commands = read_commands(config)
    o = execute_shell_script(InstanceId=i_id, config=config, commands=commands, sleep=120, n_retry=25)
    terminate_instances(config)