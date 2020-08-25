CREATE TABLE songplays
    (
        start_time timestamp,
        user_id integer,
        level text,
        song_id integer,
        artist_id integer,
        session_id integer,
        location text,
        user_agent text,
        PRIMARY KEY (start_time, user_id)
);