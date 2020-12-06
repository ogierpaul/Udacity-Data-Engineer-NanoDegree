from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.hooks.


# Parameters:
arn = os.environ.get('AWS_ARN')
aws_credentials_id = 'aws_credentials'
conn_id = 'redshift'
region = 'eu-central-1'
s3_bucket = "udacity-dend"

start_date = datetime.utcnow()

default_args = {
    'owner': 'paul ogier',
    'start_date': datetime(2018, 5, 1),
    'end_date': datetime(2020, 12, 30),
    'depends_on_past': False,
    'retries': None,
    'retry_delay': None,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udacity_capstone',
          default_args=default_args,
          description='Load and transform data in Redshift from S3 bucket',
          schedule_interval='@once',
          max_active_runs=2
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

LaunchEc2 =

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> end_operator
