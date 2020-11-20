CREATE TABLE songs (
    song_id VARCHAR(64),
    title VARCHAR(256),
    artist_id VARCHAR(64),
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
);
