# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS  songplay;"
user_table_drop = "DROP TABLE IF EXISTS  users;"
song_table_drop = "DROP TABLE IF EXISTS  songs;"
artist_table_drop = "DROP TABLE IF EXISTS  artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
staging_events_table_create = ("""
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
CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id BIGINT IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
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
DISTKEY (artist_id)
SORTKEY (artist_id, year);
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

# Query list
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]