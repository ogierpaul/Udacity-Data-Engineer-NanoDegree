TRUNCATE {schemaint}.{table};

COPY {schemaint}.{table}
FROM {inputpath}
IAM_ROLE AS {arn}
REGION {region}
COMPUPDATE OFF
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto';