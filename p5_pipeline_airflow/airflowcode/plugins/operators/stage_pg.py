from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToPGOperator(BaseOperator):
    """
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 source="",
                 pg_conn_id="",
                 pg_table="",
                 pg_stage_area="",
                 query="",
                 *args, **kwargs):

        super(StageToPGOperator, self).__init__(*args, **kwargs)
        self.pg_conn_id = pg_conn_id
        self.source = source
        self.pg_table = pg_table
        self.pg_stage_area = pg_stage_area
        self.query=query

    def execute(self, context):
        self.log.info('Executing StageToPGOperator')
        self
