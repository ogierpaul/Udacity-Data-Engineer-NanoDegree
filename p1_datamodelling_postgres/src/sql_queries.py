# HERE we define all sql_queries used in create_tables.py and etl.py

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS  songplays"
user_table_drop = "DROP TABLE IF EXISTS  users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS  TIME"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays
    (
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id INTEGER,
        artist_id INTEGER,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (start_time, user_id)
);

""")

user_table_create = ("""
CREATE TABLE users (
    user_id INTEGER,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    gender CHAR(1),
    level VARCHAR(32),
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR(64),
    title VARCHAR(64),
    artist_id VARCHAR(64),
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(64),
    name VARCHAR(256),
    location VARCHAR (256),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO
    songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time, user_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id)
    DO NOTHING ;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id)
    DO NOTHING ;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT (start_time)
    DO NOTHING ;
""")

# FIND SONGS

song_select = ("""
SELECT a1.song_id,a2.artist_id 
                    FROM songs a1 
                    JOIN artists a2 USING (artist_id) 
                    WHERE 
                    a1.title=(%s) AND 
                    a2.name=(%s) AND 
                    a1.duration=(%s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
