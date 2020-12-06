DROP TABLE IF EXISTS {table};

CREATE TABLE IF NOT EXISTS {table} (
    id INTEGER,
    objet VARCHAR(256)
);

COPY {table}
FROM {s3_location}
IAM_ROLE AS 'arn:aws:iam::075227836161:role/redshiftwiths3'
FORMAT AS JSON {s3_jsonpath}