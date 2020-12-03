from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2 import sql as S


class LoadFactOperator(BaseOperator):
    ui_color = '#a0c4ff'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 conn_id="",
                 query="",
                 truncate=True,
                 table="",
                 *args, **kwargs):
        """

        Args:
            aws_credentials_id (str): AWS credentials name in Airflow
            conn_id (str): Redshift connection name in Airflow
            query (str): SQL query to load date
            truncate (bool): if True, will truncate before loading, else will simply append
            table (str): table name
            *args:
            **kwargs:
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_id = aws_credentials_id
        self.conn_id = conn_id
        self.qf_load = S.SQL("""
        INSERT INTO {table}
        {query};
        """).format(table=S.Identifier(table), query=S.SQL(query))
        self.truncate = truncate
        self.table = table
        self.qf_truncate = S.SQL("TRUNCATE {table};").format(table=S.Identifier(self.table))

    def execute(self, context):
        """
        Run the query
        Args:
            context:

        Returns:

        """
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info('Loading fact table')
        if self.truncate:
            redshift_hook.run((self.qf_truncate, self.qf_load))
        else:
            redshift_hook.run((self.qf_load,))
        self.log.info('Load done')
