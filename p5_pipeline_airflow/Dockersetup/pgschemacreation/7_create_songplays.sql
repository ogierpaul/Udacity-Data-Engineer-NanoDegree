CREATE TABLE songplays
    (
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR(64),
        artist_id VARCHAR(64),
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (start_time, user_id)
);
