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
       data ->> 'codecpv'                                       as "codecpv",
       CAST(data ->> 'dureemois' AS INTEGER)                    as "dureemois",
       TO_DATE(data ->> 'datenotification', 'YYYY-MM-DD')       as "datenotification",
       TO_DATE(data ->> 'datepublicationdonnees', 'YYYY-MM-DD') as "datepublicationdonnees",
       CAST(data ->> 'montant' AS DOUBLE PRECISION)             as montant,
       data ->> 'formeprix'                                     as "formeprix",
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