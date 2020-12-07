CREATE TABLE IF NOT EXISTS decp_json
(
    data jsonb NOT NULL
);

COPY decp_json FROM '/data/15-DECP/1-DECP/jq_decp.json' WITH (FORMAT CSV, QUOTE E'\x1f', delimiter E'\x1e');



DROP TABLE IF EXISTS decp_attributes;

CREATE TABLE IF NOT EXISTS decp_attributes
(
    "decp_uid"               VARCHAR(128) PRIMARY KEY,
    "source"                 varchar(64),
    "decp_id"                VARCHAR(64),
    "type"                   varchar(64),
    "nature"                 varchar(64),
    "procedure"              varchar(128),
    "objet"                  VARCHAR(256),
    "codecpv"                varchar(16),
    "dureemois"              INTEGER,
    "datenotification"       DATE,
    "datepublicationdonnees" DATE,
    "montant"                DOUBLE PRECISION,
    "formeprix"              varchar(64),
    "acheteur_id"            VARCHAR(128),
    "acheteur_name"          varchar(256),
    "n_titulaires"           INTEGER
);

INSERT INTO decp_attributes
SELECT data ->> 'uid'                                           as "decp_uid",
       data ->> 'source'                                        as "source",
       data ->> 'id'                                            as "decp_id",
       data ->> '_type'                                         as "type",
       data ->> 'nature'                                        as "nature",
       data ->> 'procedure'                                     as "procedure",
       data ->> 'objet'                                         as "objet",
       data ->> 'codeCPV'                                       as "codecpv",
       CAST(data ->> 'dureeMois' AS INTEGER)                    as "dureemois",
       TO_DATE(data ->> 'dateNotification', 'YYYY-MM-DD')       as "datenotification",
       TO_DATE(data ->> 'datePublicationDonnees', 'YYYY-MM-DD') as "datepublicationdonnees",
       CAST(data ->> 'montant' AS DOUBLE PRECISION)             as montant,
       data ->> 'formePrix'                                     as "formeprix",
       data -> 'acheteur' ->> 'id'                              as "acheteur_id",
       data -> 'acheteur' ->> 'nom'                             as "acheteur_name",
       jsonb_array_length(data -> 'titulaires')                 as n_titulaires
from decp_json;


CREATE TABLE decp_titulaires
(
    "decp_titulaire_uid"       UUID PRIMARY KEY,
    "decp_uid"                  VARCHAR(128),
    "titulaire_id"              VARCHAR(128),
    "titulaire_name"            VARCHAR(256),
    "titulaire_typeidentifiant" VARCHAR(64)
);


//todo: Deduplicate on id and uid
// clean 99999 into Null
// Remove blanks and points

INSERT INTO decp_titulaires
SELECT CAST(md5(CAST(decp_uid AS VARCHAR) || CAST(titulaire_id AS VARCHAR)  || CAST(titulaire_name AS VARCHAR) ) AS UUID)as decp_titulaire_uid,
       decp_uid,
       titulaire_id,
       titulaire_name,
       titulaire_typeidentifiant
FROM (SELECT DISTINCT decp_uid, titulaire_id, titulaire_name, titulaire_typeidentifiant
        FROM
                      (SELECT data ->> 'uid'                                                      as "decp_uid",
                              jsonb_path_query(data, '$.titulaires[*]') ->> 'id'                  as titulaire_id,
                              jsonb_path_query(data, '$.titulaires[*]') ->> 'denominationSociale' as titulaire_name,
                              jsonb_path_query(data, '$.titulaires[*]') ->> 'typeIdentifiant'     as titulaire_typeidentifiant
                       FROM decp_json
                      ) b
     ) c
WHERE c.titulaire_id IS NOT NULL;






COPY decp_attributes TO '/data/15-DECP/9-output/decp_attributes.csv' CSV DELIMITER '|' HEADER QUOTE '"';
COPY decp_titulaires TO '/data/15-DECP/9-output/decp_titulaires.csv' CSV DELIMITER '|' HEADER QUOTE '"';




(SELECT
       DISTINCT
        CASE
           WHEN titulaire_typeidentifiant = 'SIRET' THEN LEFT(titulaire_id, 9)
           WHEN Left(titulaire_typeidentifiant, 3) = 'TVA' AND LEFT(titulaire_id, 2) = 'FR' THEN  substr(titulaire_id, 5, 9)
           ELSE NULL
        END as siren
FROM
     (SELECT DISTINCT  titulaire_typeidentifiant, REPLACE(REPLACE(titulaire_id, ' ',''),'.', '') as titulaire_id
    FROM decp_titulaires
     WHERE titulaire_typeidentifiant = 'SIRET'
     OR (Left(titulaire_typeidentifiant, 3) = 'TVA' AND LEFT(titulaire_id, 2) = 'FR')
        OR titulaire_id = '999999999'
         OR titulaire_id IS NULL
         ) b
) c;


COPY decp_titulaires
FROM 's3://paulogiereucentral1/decp_titulaires.csv'
IAM_ROLE 'arn:aws:iam::075227836161:role/iacredshifts3access'
FORMAT AS CSV IGNOREHEADER AS 1 DELIMITER AS '|';


CREATE TABLE decp_supplier
(
    decp_uid    varchar(128),
    siren       VARCHAR(9),
    eu_vat      VARCHAR(32),
    countrycode VARCHAR(2)
);


INSERT INTO decp_supplier
SELECT decp_uid,
       CASE
           WHEN titulaire_typeidentifiant = 'SIRET' THEN LEFT(titulaire_id, 9)
           WHEN Left(titulaire_typeidentifiant, 3) = 'TVA' AND LEFT(titulaire_id, 2) = 'FR'
               THEN substring(titulaire_id, 5, 9)
           ELSE NULL
           END as siren,
        CASE
            WHEN LEFT(titulaire_typeidentifiant, 3) = 'TVA' THEN titulaire_id
            WHEN titulaire_typeidentifiant = 'SIRET' THEN 'FR' || LPAD(CAST(MOD(12 + 3*MOD(CAST(LEFT(titulaire_id, 9) AS INTEGER), 97), 97) AS VARCHAR), 2) || titulaire_id
            ELSE NULL
        END as eu_vat,
       CASE
            WHEN LEFT(titulaire_typeidentifiant, 3) = 'TVA' THEN LEFT(titulaire_id,2)
            WHEN titulaire_typeidentifiant = 'SIRET' THEN 'FR'
            ELSE NULL
        END as countrycode
FROM (SELECT DISTINCT decp_uid,
                      titulaire_typeidentifiant,
                      titulaire_id
      FROM
           (SELECT decp_uid,
               titulaire_typeidentifiant,
                REPLACE(REPLACE(titulaire_id, ' ', ''),'.','') as titulaire_id
           FROM decp_titulaires) b
      WHERE NOT (titulaire_id = '999999999' OR titulaire_id IS NULL OR titulaire_id = '')
        AND (
              (titulaire_typeidentifiant = 'SIRET' AND LENGTH(titulaire_id) <=14 AND (titulaire_id ~ '^[0-9]*$'))
              OR
              (LEFT(titulaire_typeidentifiant, 3) = 'TVA' AND titulaire_id ~ '^[A-z][A-z]')
          )
     ) c;

