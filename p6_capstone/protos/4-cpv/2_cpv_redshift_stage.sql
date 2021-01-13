INSERT INTO {schemaout}.cpv
SELECT
    codecpv,
    description
FROM
    {schemaint}.cpv
INNER JOIN (SELECT DISTINCT codecpv from {schemaout}.decp_marches WHERE NOT codecpv IS NULL) b USING (codecpv);
;
