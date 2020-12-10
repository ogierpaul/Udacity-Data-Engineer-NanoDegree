CREATE TEMPORARY TABLE IF NOT EXISTS decp_titulaires_trimmed_ids
(
    "decp_uid" VARCHAR(128),
    "titulaire_id" VARCHAR(64),
    "titulaire_name" VARCHAR(256),
    "titulaire_typeidentifiant" VARCHAR(64)
);


TRUNCATE decp_titulaires_trimmed_ids;

INSERT INTO decp_titulaires_trimmed_ids
SELECT decp_uid,
     UPPER(REPLACE(REPLACE(TRIM(titulaire_id), ' ', ''), '.', '')) as titulaire_id,
   TRIM(titulaire_name) as titulaire_name,
     CASE
         WHEN UPPER(titulaire_typeidentifiant) = 'SIRET' THEN 'SIRET'
        WHEN UPPER(LEFT(titulaire_typeidentifiant, 3)) = 'TVA' THEN 'TVA'
    END as titulaire_typeidentifiant
FROM {schemaint}.staging_decp_titulaires
WHERE
      (NOT (titulaire_id IS NULL OR titulaire_id = '999999999' OR titulaire_id = ''))
    AND
      (titulaire_typeidentifiant = 'SIRET' OR Left(titulaire_typeidentifiant, 3) = 'TVA');

CREATE TEMPORARY TABLE IF NOT EXISTS decp_titulaires_valid_ids
(
    "decp_uid" VARCHAR(128),
    "titulaire_id" VARCHAR(64),
    "titulaire_name" VARCHAR(256),
    "titulaire_typeidentifiant" VARCHAR(64),
    "siren" VARCHAR(9)
);

TRUNCATE decp_titulaires_valid_ids;

INSERT INTO decp_titulaires_valid_ids
SELECT
decp_uid,
titulaire_id,
   titulaire_name,
   CASE
       WHEN titulaire_typeidentifiant = 'TVA' THEN 'TVA'
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =14) THEN 'SIRET'
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =9) THEN 'SIREN'
    END as titulaire_typeidentifiant,
    CASE
       WHEN titulaire_typeidentifiant = 'TVA' AND LEFT(titulaire_id, 2) = 'FR' THEN SUBSTRING(titulaire_id, 5, 9)
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =14) THEN LEFT(titulaire_id, 9)
        WHEN (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) =9) THEN titulaire_id
        ELSE NULL
    END as siren
FROM (
     SELECT *
     FROM decp_titulaires_trimmed_ids
     WHERE
         (
            titulaire_typeidentifiant = 'SIRET'
           AND (LENGTH (titulaire_id) = 14 OR LENGTH (titulaire_id) = 9)
           AND (titulaire_id ~ '^[0-9]*$')
       )
        OR
         (
         titulaire_typeidentifiant = 'TVA'
        AND titulaire_id ~ '^[A-z][A-z]'
        )
 ) b;

TRUNCATE  {schemaint}.decp_titulaires_formatted;

INSERT INTO  {schemaint}.decp_titulaires_formatted
SELECT
       decp_uid,
       CASE
           WHEN titulaire_typeidentifiant = 'TVA' THEN titulaire_id
           WHEN siren IS NOT NULL THEN 'FR' || LPAD(
                            CAST(MOD(12 + 3 * MOD(CAST(siren AS INTEGER), 97), 97) AS VARCHAR), 2, '0')  || siren
        END as eu_vat,
       siren,
       CASE WHEN titulaire_typeidentifiant = 'SIRET' THEN titulaire_id ELSE NULL END as siret,
       titulaire_name
FROM decp_titulaires_valid_ids;


TRUNCATE table {schemaout}.decp_marches;

INSERT INTO {schemaout}.decp_marches
SELECT *
FROM {schemaint}.staging_decp_marches;

TRUNCATE {schemaout}.decp_awarded;
INSERT INTO {schemaout}.decp_awarded
SELECT decp_uid, eu_vat, siren, siret, titulaire_name, LEFT(eu_vat, 2) as countrycode
FROM {schemaint}.decp_titulaires_formatted;
