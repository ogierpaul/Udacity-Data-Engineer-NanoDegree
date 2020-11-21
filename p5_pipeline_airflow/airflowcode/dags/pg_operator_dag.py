from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import PgQueries
from airflow.operators.postgres_operator import PostgresOperator

###
# For testing and learning purposes
# This DAGS run the data ingestion steps into a PostGreSQL Database
# Prerequisites
# Postgre connection registered as 'mypg'
# Staging files accessible by Postgres under /data/stagingarea
# SQL queries are defined in airflowcode/plugins/helpers
###

default_args = {
    'owner': 'paulogier',
    'start_date': datetime(2019, 1, 12)
}

dag = DAG('pg__operator_dag',
          default_args=default_args,
          description='Load and transform data in PG from staging files',
          schedule_interval=None
          )

Start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

Stage_events = PostgresOperator(
    postgres_conn_id='mypg',
    task_id='Stage_events',
    dag=dag,
    sql=PgQueries.staging_events_copy
)

Stage_songs = PostgresOperator(
    postgres_conn_id='mypg',
    task_id='Stage_songs',
    dag=dag,
    sql=PgQueries.staging_songs_copy
)

load_songplays_table = PostgresOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.songplay_table_insert
)

load_user_dimension_table = PostgresOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.user_table_insert
)

load_song_dimension_table = PostgresOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.songs_table_insert
)

load_artist_dimension_table = PostgresOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.artists_table_insert
)

load_time_dimension_table = PostgresOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.time_table_insert
)

delete_staging_songs = PostgresOperator(
    task_id='Delete_staging_songs',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.staging_songs_delete
)

delete_staging_events = PostgresOperator(
    task_id='Delete_staging_events',
    dag=dag,
    postgres_conn_id='mypg',
    sql=PgQueries.staging_events_delete
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

Start_operator >> [Stage_songs, Stage_events]
Stage_songs >> [load_artist_dimension_table, load_song_dimension_table] >> delete_staging_songs
Stage_events >> [load_user_dimension_table, load_time_dimension_table]
[load_artist_dimension_table, load_song_dimension_table, Stage_events] >> load_songplays_table >> delete_staging_events>> end_operator
