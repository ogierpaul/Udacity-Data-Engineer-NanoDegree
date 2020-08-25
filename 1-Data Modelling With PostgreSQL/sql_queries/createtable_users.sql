CREATE TABLE users (
    user_id INTEGER,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    gender CHAR(1),
    level VARCHAR(32),
    PRIMARY KEY (user_id)
);

/* EXAMPLE AND TEST */
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (1, 'foo', 'bar', 'M', 'free');

SELECT * FROM users;

DELETE FROM users;
