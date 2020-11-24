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
    ui_color = '#89DA59'

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
            context:

        Returns:

        """
        self.log.info('Creating schema..')
        self.log.info(os.listdir(os.getcwd()))
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        f = open(CreateSchemaOperator.sql_statements_path, 'r')
        sql_all = f.read()
        f.close()

        sql_commands = sql_all.split(';')

        for command in sql_commands:
            if command.rstrip() != '':
                hook.run((S.SQL(command+';'), ))
        self.log.info('Schema created')