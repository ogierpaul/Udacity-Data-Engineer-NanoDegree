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
    copy_statement = os.path.join(os.path.dirname(os.path.abspath(__file__)), 's3_copy.sql')
    d = [
        {'table': 'staging_decp_marches', 'inputpath': 's3://paulogiereucentral1/p6/decp/output/marches.json'},
        {'table': 'staging_decp_titulaires', 'inputpath': 's3://paulogiereucentral1/p6/decp/output/titulaires.json'}
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
    execute_statements(
        filepath=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'decp_redshift_stage.sql'),
        config=config,
        params={
            'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
            'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
        }
    )
def load_to_schemaout(config):
    #TODO: Write those statements
    execute_statements(
        filepath=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'decp_load_out.sql'),
        config=config,
        params={
            'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
            'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT"))
        }
    )



if __name__ == '__main__':
    config = get_project_config(config_path)
    load_from_s3(config)
    stage_inside_redshift(config)
    #TODO: Rewrite "myext".decp_marches column "datenotification" is of type date but expression is of type character varying
    load_to_schemaout(config)
    print('done')
