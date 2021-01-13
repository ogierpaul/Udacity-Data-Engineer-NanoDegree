from dendutils.config import get_project_config
from dendutils.redshift import execute_statements, get_conn
import psycopg2.sql as S
import os

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')

config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p6_capstone/config/config_path.cfg'
def unload(config):
    statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'unload.sql')
    tables = ['decp_awarded', 'decp_marches', 'siren_directory', 'siren_present']
    for t in tables:
        outputpath = '/'.join(["s3:/", config.get("S3","bucket"), config.get("S3", "decpoutputfolder"), t + '/'])
        params = {
            'schema': S.Identifier(config.get("DB", "SCHEMA_OUT")),
            'arn': S.Literal(config.get("IAM", "CLUSTER_IAM_ARN")),
            'region': S.Literal(config.get("REGION", "REGION")),
            'table': S.Identifier(t),
            'outputpath': S.Literal(outputpath)
        }
        execute_statements(filepath=statement, config=config, params=params)
    return None

if __name__ == '__main__':
    config = get_project_config(config_path)
    unload(config)