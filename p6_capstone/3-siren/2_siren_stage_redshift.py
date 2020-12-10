from dendutils import get_project_config
from dendutils.redshift import execute_statements
import psycopg2.sql as S
import os

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')

config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p6_capstone/config/config_path.cfg'
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

def transform_inside_redshift(config):
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'siren_load.sql')
    params = {
        'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
    }
    execute_statements(filepath=copy_statement, config=config, params=params)

if __name__ == '__main__':
    config = get_project_config(config_path)
    #TODO: Add SSH to EC2 to download, unzip and output to s3
    load_from_s3(config)
    transform_inside_redshift(config)