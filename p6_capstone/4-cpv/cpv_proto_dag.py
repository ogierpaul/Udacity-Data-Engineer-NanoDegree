from dendutils.config import get_project_config
from dendutils.redshift import execute_statements, getOrCreate
from s3 import upload_file
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

config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/config/config_path.cfg'

def upload_cpv_file(config):
    """
    Copy the jq statements from this module to s3 for import into ec2
    Args:
        config: paths [S3, BUCKET], [S3, DECP_CONFIGFOLDER]

    Returns:

    """
    logger.info('starting upload of cpv dictionnary file to s3')
    moddir = os.path.dirname(os.path.abspath(__file__))
    s3_bucket = config.get('S3', 'BUCKET')
    s3_folder = config.get('S3', 'CPV_CONFIGFOLDER')
    f = os.path.join(moddir, 'cpv_2008_ver_2013.csv')
    object_name = s3_folder.rstrip('/') + '/' + f.split('/')[-1]
    upload_file(config, bucket=s3_bucket, file_name=f, object_name=object_name)
    logger.info(f'end upload of {f}')
    return None


def copy_from_s3(config):
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), '1_cpv_copy_s3.sql')
    s3_bucket = config.get("S3", "BUCKET")
    s3_outputfolder =  config.get("S3", "CPV_OUTPUTFOLDER")
    s3_bucket = s3_bucket.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    output_cpv_s3 = 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'cpv_2008_ver_2013.csv',
    params = {
        'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'arn': S.Literal(config.get("IAM", "CLUSTER_IAM_ARN")),
        'region': S.Literal(config.get("REGION", "REGION")),
        'table': S.Identifier('cpv'),
        'inputpath': S.Literal(output_cpv_s3)
    }
    execute_statements(filepath=copy_statement, config=config, params=params)
    return None

def stage_inside_redshift(config):
    execute_statements(
        filepath=os.path.join(os.path.dirname(os.path.abspath(__file__)), '2_cpv_redshift_stage.sql'),
        config=config,
        params={
            'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
            'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
        }
    )


if __name__ == '__main__':
    logger.info("Starting main")
    config = get_project_config(config_path)
    rs = getOrCreate(config)
    upload_cpv_file(config)
    copy_from_s3(config)
    stage_inside_redshift(config)
    print('done')





