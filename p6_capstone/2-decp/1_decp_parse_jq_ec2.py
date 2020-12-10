from dendutils.config import get_project_config
from dendutils.ec2 import create_vm, execute_shell_script
from s3 import upload_file
from botocore.exceptions import ClientError
import os
import time
import logging


import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')

config_path = '/p6_capstone/config_path.cfg'

def upload_data(config, s3_bucket, s3_folder):
    logger.info('starting upload')
    #Moddit is the module directory
    moddir = os.path.dirname(os.path.abspath(__file__))
    #TODO: Add acheteurs to list of fields name returned
    jq_marches = os.path.join(moddir, 'jq_transfo_marches.sh')
    jq_titulaires = os.path.join(moddir, 'jq_transfo_titulaires.sh')
    for f in [jq_marches, jq_titulaires]:
        object_name = s3_folder.rstrip('/') + '/' + f.split('/')[-1]
        #TODO: Review file paths
        upload_file(config, bucket=s3_bucket, file_name=f, object_name=object_name)
        logger.info(f'end download of {f}')
    return None


if __name__ == '__main__':
    logger.info("Starting main")
    config = get_project_config(config_path)
    j = create_vm(config)
    #TODO: Check that the right level is returned by create: j or j[0]
    upload_data(config, config.get("S3", "bucket"), config.get("S3", "decpinputfolder"))
    #TODO: Write sh_all
    sh_all = 'TODO'
    try:
        o = execute_shell_script(i_id=j.id, config=config, commands=sh_all, sleep=30)
        logger.info(o['StandardOutputContent'])
    except ClientError as e:
        logger.info(e)
    try:
        o = j.terminate()
        logger.info(f"terminating instance {j.id}")
        logger.info(o)
    except ClientError as e:
        logger.info(e)

#
#
#
#
