from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import logging
outputfolder = '/stagingarea/'

dag = DAG("load_songs",
          start_date=days_ago(7),
          schedule_interval=None
          )

q_copy = """
COPY staging_songs (
    artist_id,
 artist_latitude,
 artist_location,
 artist_longitude,
 artist_name,
 duration,
 song_id,
 title, 
 year)
FROM '/stagingarea/songs.csv'
WITH CSV HEADER DELIMITER '|';
"""

q_songs_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
ON CONFLICT (song_id)
    DO NOTHING;
"""

q_artists_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
FROM staging_songs
ON CONFLICT (artist_id)
    DO NOTHING;
"""

q_stagingsongs_delete = """
DELETE FROM staging_songs
"""

copyrawsongs = PostgresOperator(
    sql=q_copy,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='copyrawsongs',
    dag=dag
)

insertsongs = PostgresOperator(
    sql=q_songs_insert,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='insertsongs',
    dag=dag
)

insertartists = PostgresOperator(
    sql=q_artists_insert,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='insertartists',
    dag=dag
)

deletestagingsongs = PostgresOperator(
    sql=q_stagingsongs_delete,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='deletestaging',
    dag=dag
)

copyrawsongs >> [insertsongs, insertartists] >> deletestagingsongs

