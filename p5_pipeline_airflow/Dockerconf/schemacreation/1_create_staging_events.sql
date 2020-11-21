CREATE TABLE IF NOT EXISTS staging_events
(
staging_event_id SERIAL,
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
registration    VARCHAR,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              VARCHAR,
userAgent       VARCHAR,
userId          INTEGER,
PRIMARY KEY (staging_event_id)
);
