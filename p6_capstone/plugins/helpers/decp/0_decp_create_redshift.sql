

CREATE TABLE IF NOT EXISTS staging.staging_decp_marches
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
    "datenotification"       VARCHAR(16),
    "datepublicationdonnees" VARCHAR,
    "montant"                DOUBLE PRECISION,
    "formeprix"              varchar(64),
    "acheteur_id" VARCHAR(128),
    "acheteur_name" VARCHAR(256)
);


CREATE TABLE IF NOT EXISTS staging.staging_decp_titulaires (
    "decp_uid" VARCHAR(128),
    "titulaire_id" VARCHAR(64),
    "titulaire_name" VARCHAR(256),
    "titulaire_typeidentifiant" VARCHAR(64)
);


CREATE TABLE IF NOT EXISTS staging.decp_titulaires_formatted (
    "decp_uid" VARCHAR(128),
    "eu_vat" VARCHAR(64),
    "siren" VARCHAR(9),
    "siret" VARCHAR(14),
    "titulaire_name" VARCHAR(256)
);


CREATE TABLE IF NOT EXISTS reporting.decp_marches(
    "decp_uid"               VARCHAR(128) PRIMARY KEY,
    "source"                 varchar(64),
    "decp_id"                VARCHAR(64),
    "type"                   varchar(64),
    "nature"                 varchar(64),
    "procedure"              varchar(128),
    "objet"                  VARCHAR(256),
    "codecpv"                varchar(16),
    "dureemois"              INTEGER,
    "datenotification"       varchar(16),
    "datepublicationdonnees" varchar(16),
    "montant"                DOUBLE PRECISION,
    "formeprix"              varchar(64),
    "acheteur_id" VARCHAR(128),
    "acheteur_name" VARCHAR(256)
);


CREATE TABLE IF NOT EXISTS reporting.decp_awarded(
    "decp_uid" VARCHAR(128),
    "eu_vat" VARCHAR(64),
    "siren" VARCHAR(9),
    "siret" VARCHAR(14),
    "titulaire_name" VARCHAR(256),
    "countrycode" VARCHAR(2),
    PRIMARY KEY (decp_uid, eu_vat)
);