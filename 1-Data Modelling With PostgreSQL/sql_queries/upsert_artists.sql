INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
VALUES ('1', 'foo', 'bar', 1.2, 2)
ON CONFLICT DO NOTHING;

