DROP TABLE IF EXISTS {schemaout}.decp_marches;

CREATE TABLE IF NOT EXISTS {schemaout}.decp_marches(
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
    "formeprix"              varchar(64)
);

DROP TABLE IF EXISTS  {schemaout}.decp_titulaires;
DROP TABLE IF EXISTS {schemaout}.decp_awarded;

CREATE TABLE IF NOT EXISTS {schemaout}.decp_awarded(
    "decp_uid" VARCHAR(128),
    "eu_vat" VARCHAR(64),
    "siren" VARCHAR(9),
    "siret" VARCHAR(14),
    "titulaire_name" VARCHAR(256),
    "countrycode" VARCHAR(2)
);

DROP TABLE IF EXISTS {schemaout}.siren_directory;

CREATE TABLE {schemaout}.siren_directory (
    siren VARCHAR(9) PRIMARY KEY,
    "siren_name" VARCHAR(256),
    status VARCHAR(32),
    removed VARCHAR(32),
    siglum VARCHAR(64),
    social_purpose VARCHAR(32),
    n_employees VARCHAR(256)
);

