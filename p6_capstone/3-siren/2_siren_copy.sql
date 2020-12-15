TRUNCATE {schemaint}.{table};

COPY {schemaint}.{table}
FROM {inputpath}
IAM_ROLE AS {arn}
REGION {region}
COMPUPDATE OFF
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS CSV IGNOREHEADER AS 1 DELIMITER AS ',';


TRUNCATE {schemaint}.siren_active;

INSERT INTO {schemaint}.siren_active
SELECT * FROM staging_siren
INNER JOIN (SELECT siren from decp_titulaires) b USING(siren);
