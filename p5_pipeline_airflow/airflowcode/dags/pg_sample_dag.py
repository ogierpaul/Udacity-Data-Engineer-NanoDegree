from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import PgQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import CreateSchemaOperator, PgStagingOperator, PgUpsertOperator

###
# For testing and learning purposes
# This DAGS run the data ingestion steps into a PostGreSQL Database
# Prerequisites
# - Postgre connection registered in Airflow
# - Staging files accessible by Postgres under /data/stagingarea (see sample data in repository)
# - SQL queries and Operators are defined in airflowcode.plugins.helpers and airflowcode.plugins.operators
# - Plugins directory availabe in $AIRFLOW_HOME

# Order of Operations (Happy Flow)
# 1. Create the Schema if not exits
# 2. Truncate staging tables and upload data from the /data/stagingarea volume
# 3. Load fact and dimension tables with upsert, check no duplicates on primary key (PgUpsertOperator)
# 4. Truncate staging tables
# 5. End
###

# Use here the conn_id (Airflow connection name, view airflow doc how to set-it up)
conn_id = 'aa_pg'

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
#
CreateSchema = CreateSchemaOperator(
    task_id='create_schema',
    conn_id=conn_id,
    dag=dag
)
#
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
    task_id='Upsert_songplays_fact_table',
    dag=dag,
    conn_id=conn_id,
    query=PgQueries.songplays_table_select,
    table='songplays',
    pkey='songplay_id'
)


load_user_dimension_table = PgUpsertOperator(
    task_id='Upsert_user_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=PgQueries.users_table_select,
    table='users',
    pkey='user_id'
)


load_song_dimension_table = PgUpsertOperator(
    task_id='Upsert_song_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=PgQueries.songs_table_select,
    table='songs',
    pkey='song_id'
)


load_artist_dimension_table = PgUpsertOperator(
    task_id='Upsert_artist_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=PgQueries.artists_table_select,
    table='artists',
    pkey='artist_id'
)

load_time_dimension_table = PgUpsertOperator(
    task_id='Upsert_time_dim_table',
    dag=dag,
    conn_id=conn_id,
    query=PgQueries.time_table_select,
    table='time',
    pkey='start_time'
)

truncate_staging_songs = PostgresOperator(
    task_id='truncate_staging_songs',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=PgQueries.staging_songs_truncate
)

truncate_staging_events = PostgresOperator(
    task_id='truncate_staging_events',
    dag=dag,
    postgres_conn_id=conn_id,
    sql=PgQueries.staging_events_truncate
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

Start_operator >> CreateSchema >> [Stage_events, Stage_songs]
Stage_events >> [load_user_dimension_table, load_time_dimension_table, load_songplays_table] >> truncate_staging_events
Stage_songs >> [load_song_dimension_table, load_artist_dimension_table, load_songplays_table] >> truncate_staging_songs
[truncate_staging_events, truncate_staging_songs] >> end_operator