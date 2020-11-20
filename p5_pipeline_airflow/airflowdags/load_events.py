from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import logging

outputfolder = '/stagingarea/'

dag = DAG("load_events",
          start_date=days_ago(7),
          schedule_interval=None
          )

q_copyevents = """
COPY staging_events (
artist,
auth,
firstName ,
gender,
itemInSession ,
lastName,
length,
level ,
location,
method,
page,
registration,
sessionId,
song,
status,
ts,
userAgent,
userId)
FROM '/stagingarea/events.csv'
WITH CSV HEADER DELIMITER '|';
"""

q_users_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
FROM (SELECT DISTINCT ON(userId) userId,  firstName, lastName, gender, level FROM staging_events WHERE userId is NOT NULL) b
ON CONFLICT (user_id)
    DO UPDATE
    SET level = excluded.level;
"""

q_time_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
                EXTRACT(HOUR from start_time) as hour,
                EXTRACT(DAY from start_time) as day,
                EXTRACT(WEEK from start_time) as week,
                EXTRACT(MONTH from start_time) as month,
                EXTRACT(YEAR from start_time) as year,
                EXTRACT(DOW from start_time) as weekday
FROM (SELECT to_timestamp( TRUNC( CAST( ts AS bigint ) / 1000 ) ) as start_time FROM staging_events
WHERE ts IS NOT NULL) b
ON CONFLICT (start_time)
    DO NOTHING;
"""

q_songplays_insert = """
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp( TRUNC( CAST( ts AS bigint ) / 1000 ) ) as start_time,
                    a.userId as user_id,
                    a.level as level,
                    b.song_id as song_id,
                    b.artist_id as artist_id,
                    a.sessionId as session_id,
                    a.location as location,
                    a.userAgent as user_agent
    FROM 
    (SELECT 
      ts,
      userId,
      sessionId,
      location,
      userAgent,
      song,
      artist,
      level
    FROM staging_events WHERE userId IS NOT NULL
    ) a
    LEFT JOIN (SELECT song_id, title, artist_id, name FROM songs c
                LEFT JOIN artists USING(artist_id)
    ) b
    ON
        a.song = b.title
    AND
        a.artist = b.name;
"""

q_stagingevents_delete = """
DELETE FROM staging_events WHERE 1=0
"""

copyrawevents = PostgresOperator(
    sql=q_copyevents,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='copyrawevents',
    dag=dag
)

insertusers = PostgresOperator(
    sql=q_users_insert,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='insertusers',
    dag=dag
)

inserttime = PostgresOperator(
    sql=q_time_insert,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='inserttime',
    dag=dag
)

insertsongplays = PostgresOperator(
    sql=q_time_insert,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='insertsongplays',
    dag=dag
)

deletestagingevents = PostgresOperator(
    sql=q_stagingevents_delete,
    postgres_conn_id='mypg',
    autocommit=True,
    task_id='deletestaging',
    dag=dag
)

copyrawevents >> [insertusers, inserttime, insertsongplays] >> deletestagingevents
