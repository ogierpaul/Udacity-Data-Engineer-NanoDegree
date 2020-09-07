import configparser
from psycopg2 import sql

# CONFIG
config = configparser.ConfigParser()
config.read('admin_config.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS  songplay;"
user_table_drop = "DROP TABLE IF EXISTS  users;"
song_table_drop = "DROP TABLE IF EXISTS  songs;"
artist_table_drop = "DROP TABLE IF EXISTS  artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
staging_event_id BIGINT IDENTITY(0,1),
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER,
PRIMARY KEY (staging_event_id)
)
DISTSTYLE EVEN;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
staging_song_id BIGINT IDENTITY(0,1),
artist_id          VARCHAR,
artist_latitude    FLOAT,
artist_location    VARCHAR,
artist_longitude   FLOAT,
artist_name        VARCHAR,
duration           FLOAT,
num_songs          INTEGER,
song_id            VARCHAR,
title              VARCHAR,
year               INTEGER,
PRIMARY KEY (staging_song_id)
)
DISTSTYLE EVEN;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id INTEGER,
        artist_id INTEGER,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (start_time, user_id)
)
DISTKEY (user_id)
SORTKEY (user_id, start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR,
    PRIMARY KEY (user_id)
)
DISTKEY (user_id)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
)
DISTKEY (song_id)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
)
DISTKEY (artist_id)
SORTKEY (artist_id)
;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
)
DISTSTYLE ALL
SORTKEY (start_time);
""")

# STAGING TABLES INSERT
staging_events_copy = """COPY staging_events
FROM '{filelocation}'
IAM_ROLE '{arn}'
COMPUPDATE OFF
REGION 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON '{jsonpath}' ;"""
#
# .format(
#     config.get("S3", "LOG_DATA"),
#     config.get('IAM_ROLE', 'ARN'),
#     config.get('S3', 'LOG_JSONPATH'),
#

staging_songs_copy = """
COPY staging_songs
FROM '{filelocation}'
IAM_ROLE '{arn}'
COMPUPDATE OFF
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto' ;
"""
#
#     .format(
#     config.get("S3", "SONG_DATA"),
#     config.get('IAM_ROLE', 'ARN')
# )

# FINAL TABLES INSERT

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                a.userId as user_id,
                a.level as level,
                b.song_id as song_id,
                b.artist_id as artist_id,
                a.sessionId as session_id,
                a.location as location,
                a.userAgent as user_agent
FROM staging_events a
JOIN staging_songs b
ON
    a.song = b.title
AND
    a.artist = b.artist_name;
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE
userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(HOUR from ts),
                EXTRACT(DAY from ts),
                EXTRACT(WEEK from ts),
                EXTRACT(MONTH from ts),
                EXTRACT(YEAR from ts),
                EXTRACT(WEEKDAY from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [
    #staging_events_copy,
    staging_songs_copy
    ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
