from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import CreateSchemaOperator, PgStagingOperator, PgUpsertOperator

###
# For testing and learning purposes
# This DAGS run the data ingestion steps into a PostGreSQL Database
# Prerequisites
# - Postgre connection registered as 'mypg'
# - Staging files accessible by Postgres under /data/stagingarea (see sample data in repository)
# - SQL queries and Operators are defined in airflowcode.plugins.helpers and airflowcode.plugins.operators
# - Plugins directory availabe in $AIRFLOW_HOME
###

pg_conn_id = 'AA_PG'
conn_id = pg_conn_id

default_args = {
    'owner': 'paulogier',
    'start_date': datetime(2019, 1, 12)
}

dag = DAG('pg_sample_dag',
          default_args=default_args,
          description='Load and transform data in PG from staging files',
          schedule_interval=None
          )

Start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

CreateSchema = CreateSchemaOperator(
    task_id='create_schema',
    conn_id=conn_id,
    dag=dag
)

Stage_events = PgStagingOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id=conn_id,
    table="staging_events",
    sourcepath="/data/stagingarea/staging_events.csv",
    delimiter='|'
)

Stage_songs = PgStagingOperator(
    dag=dag,
    conn_id=conn_id,
    task_id='Stage_songs',
    table='staging_songs',
    sourcepath="/data/stagingarea/staging_songs.csv",
    delimiter='|'
)

load_songplays_table = PgUpsertOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.songplays_table_insert,
    table='songplays'
)

load_user_dimension_table = PgUpsertOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.users_table_insert,
    table='users'
)

load_song_dimension_table = PgUpsertOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.songs_table_insert,
    table='songs'
)

load_artist_dimension_table = PgUpsertOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.artists_table_insert,
    table='artists'
)

load_time_dimension_table = PgUpsertOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=SqlQueries.time_table_insert,
    table='time'
)

delete_staging_songs = PostgresOperator(
    task_id='Delete_staging_songs',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=SqlQueries.staging_songs_truncate
)

delete_staging_events = PostgresOperator(
    task_id='Delete_staging_events',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=SqlQueries.staging_events_truncate
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

Start_operator >> CreateSchema >> [Stage_songs, Stage_events]
Stage_songs >> [load_artist_dimension_table, load_song_dimension_table] >> delete_staging_songs
Stage_events >> [load_user_dimension_table, load_time_dimension_table]
[load_artist_dimension_table, load_song_dimension_table, Stage_events] >> load_songplays_table >> delete_staging_events
[delete_staging_songs, delete_staging_events] >>end_operator