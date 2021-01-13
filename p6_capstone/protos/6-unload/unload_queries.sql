UNLOAD ('select * FROM {schema}.{table}')
TO {outputpath}
IAM_ROLE AS {arn}
HEADER
CSV DELIMITER AS '|' ALLOWOVERWRITE;
