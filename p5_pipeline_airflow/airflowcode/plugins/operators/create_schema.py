from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S
import os

class CreateSchemaOperator(BaseOperator):
    """
    The sql_statements file contains the queries used to create the schema
    Make sure they are separated by ;
    """
    sql_statements_path = 'plugins/operators/create_tables.sql'
    ui_color = '#ffd6a5'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 *args, **kwargs):

        super(CreateSchemaOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id


    def execute(self, context):
        """
        Executes all the queries in the "sql_statements" file
        - Used to create a schema in the Database
        Args:
            context: See Airflow context

        Returns:

        """
        self.log.info('Creating schema..')
        self.log.info(os.listdir(os.getcwd()))
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        # Create a list of sql commands from the sql_statements file
        f = open(CreateSchemaOperator.sql_statements_path, 'r')
        sql_all = f.read()
        f.close()
        q_all = [S.SQL(q + ';') for q in sql_all.split(';') if q.rstrip()!= '']

        # Execute them
        hook.run(q_all)

        self.log.info('Schema created')