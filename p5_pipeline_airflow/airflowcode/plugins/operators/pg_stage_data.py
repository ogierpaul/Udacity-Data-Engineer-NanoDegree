from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S

class PgStagingOperator(BaseOperator):
    """
    This operator will COPY FROM a CSV
    """
    ui_color = '#358140'
    query =   """
    COPY {table}
    FROM {sourcepath}
    WITH CSV HEADER DELIMITER {delimiter}
    """

    @apply_defaults
    def __init__(self,
                 sourcepath="",
                 conn_id="",
                 delimiter=",",
                 table="",
                 *args, **kwargs):
        """

        Args:
            sourcepath (str): path to file
            conn_id (str): Airflow connection ID
            delimiter (str): CSV delimiter
            table (str): PG table name
            *args:
            **kwargs:
        """

        super(PgStagingOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.delimiter = delimiter
        self.sourcepath = sourcepath
        self.table = table

    def execute(self, context):
        """
        COPY table from a CSV file
        - truncate table
        - execute copy with arguments:
        - COPY table FROM sourcepath WITH CSV HEADER DELIMITER delimiter
        Args:
            context:

        Returns:
            None
        """
        self.log.info('Executing StagingOperator')
        self.log.info('Creating hook')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(sql=(S.SQL("truncate {}").format(S.Identifier(self.table)), ))
        hook.run(
             sql=(
                 S.SQL(PgStagingOperator.query).format(
                     table=S.Identifier(self.table),
                     sourcepath=S.Literal(self.sourcepath),
                     delimiter=S.Literal(self.delimiter)
                 ),
            )
        )
        self.log.info('Ran SQL query')
        pass
