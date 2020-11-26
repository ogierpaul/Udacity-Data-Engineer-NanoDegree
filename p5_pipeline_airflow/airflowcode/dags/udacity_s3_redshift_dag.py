from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import RedshiftStagingOperator, CreateSchemaOperator, RsUpsertOperator
import os

# Parameters:
arn = os.environ.get('AWS_ARN')
conn_id = 'aa_rs'
region = 'us-west-2'
song_path = "s3://udacity-dend/song_data/A/A/A"
log_path = "s3://udacity-dend/log_data/"

default_args = {
    'owner': 'paulogier',
    'start_date': datetime(2019, 1, 12)
}


# Submission to the Udacity Project
# This DAGS ETL the data from S3 to Redshift
# Prerequisites
# - Redshift registered in Airflow
# - Redshift has rights (ARN) to access S3
# - SQL queries and Operators are defined in airflowcode.plugins.helpers and airflowcode.plugins.operators
# - Plugins directory availabe in $AIRFLOW_HOME
# Order of Operations (Happy Flow)
# 1. Create the Schema if not exits
# 2. Truncate staging tables and upload data from S3 (RedshiftStagingOperator)
# 3. Load fact and dimension tables with upsert, check no duplicates on primary key (RsUpsertOperator)
# 4. Truncate staging tables
# 5. End


dag = DAG('udacity_s3_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift from S3 bucket',
          schedule_interval=None
          )

Start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

CreateSchema = CreateSchemaOperator(
    task_id='create_schema',
    conn_id=conn_id,
    dag=dag
)

Stage_events = RedshiftStagingOperator(
    task_id='Stage_events',
    dag=dag,
    arn=arn,
    conn_id=conn_id,
    region=region,
    table="staging_events",
    path=log_path,

)

Stage_songs = RedshiftStagingOperator(
    task_id='Stage_songs',
    dag=dag,
    arn=arn,
    conn_id=conn_id,
    region=region,
    table="staging_songs",
    path=song_path

)

load_songplays_table = RsUpsertOperator(
    task_id='Upsert_songplays_fact_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.songplays_table_select,
    table='songplays',
    pkey='songplay_id'
)

load_user_dimension_table = RsUpsertOperator(
    task_id='Upsert_user_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.users_table_select,
    table='users',
    pkey='user_id'
)

load_song_dimension_table = RsUpsertOperator(
    task_id='Upsert_song_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.songs_table_select,
    table='songs',
    pkey='song_id'
)

load_artist_dimension_table = RsUpsertOperator(
    task_id='Upsert_artist_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.artists_table_select,
    table='artists',
    pkey='artist_id'
)

load_time_dimension_table = RsUpsertOperator(
    task_id='Upsert_time_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.time_table_select,
    table='time',
    pkey='start_time'
)

truncate_staging_songs = PostgresOperator(
    task_id='truncate_staging_songs',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=SqlQueries.staging_songs_truncate
)

truncate_staging_events = PostgresOperator(
    task_id='truncate_staging_events',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=SqlQueries.staging_events_truncate
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

Start_operator >> CreateSchema >> [Stage_songs, Stage_events]
Stage_events >> [load_user_dimension_table, load_time_dimension_table, load_songplays_table] >> truncate_staging_events
Stage_songs >> [load_artist_dimension_table, load_song_dimension_table, load_songplays_table] >> truncate_staging_songs
[truncate_staging_events, truncate_staging_songs] >> end_operator
