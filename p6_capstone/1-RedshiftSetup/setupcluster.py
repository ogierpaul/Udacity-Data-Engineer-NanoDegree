from dendutils import get_project_config, create_cluster_main
from dendutils.redshift import execute_statements
import psycopg2.sql as S
from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)
import boto3
import os
import time
import logging
from botocore.exceptions import ClientError

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(format=log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')
config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p6_capstone/config/config_path.cfg'

if __name__ == '__main__':
    config = get_project_config(config_path)
    # mycluster = create_cluster_main(config=config, sleep=180)
    #TODO: Add a check if the cluster is up and running
    params = {
        'schemaint': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'schemaout': S.Identifier(config.get("DB", "SCHEMA_OUT")),
        'ro_user_name': S.Identifier(config.get("DB", "RO_USER")),
        'ro_user_password': S.Literal(config.get("DB", "RO_PASSWORD"))
    }

    for f in ['1_create_schemas.sql', '2_create_users.sql', '3_create_staging.sql', '4_create_out.sql']:
        logger.info(f'Executing file {f}')
        execute_statements(
            filepath=os.path.join(os.path.dirname(os.path.abspath(__file__)), f),
            config=config,
            params=params
        )
    logger.info(f'Done')



