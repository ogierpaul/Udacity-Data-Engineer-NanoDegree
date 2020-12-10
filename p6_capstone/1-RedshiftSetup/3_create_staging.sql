DROP TABLE IF EXISTS {schemaint}.staging_decp_marches;

CREATE TABLE IF NOT EXISTS {schemaint}.staging_decp_marches
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
    "formeprix"              varchar(64)
);

DROP TABLE IF EXISTS {schemaint}.staging_decp_titulaires;

CREATE TABLE IF NOT EXISTS {schemaint}.staging_decp_titulaires (
    "decp_uid" VARCHAR(128),
    "titulaire_id" VARCHAR(64),
    "titulaire_name" VARCHAR(256),
    "titulaire_typeidentifiant" VARCHAR(64)
);

DROP TABLE IF EXISTS {schemaint}.decp_titulaires_formatted;

CREATE TABLE IF NOT EXISTS {schemaint}.decp_titulaires_formatted (
    "decp_uid" VARCHAR(128),
    "eu_vat" VARCHAR(64),
    "siren" VARCHAR(9),
    "siret" VARCHAR(14),
    "titulaire_name" VARCHAR(256)
);


DROP TABLE IF EXISTS {schemaint}.staging_siren;

CREATE TABLE IF NOT EXISTS {schemaint}.staging_siren(
    siren                                     VARCHAR(256) PRIMARY KEY ,
    statutDiffusionUniteLegale                VARCHAR(256),
    unitePurgeeUniteLegale                    VARCHAR(256),
    dateCreationUniteLegale                   VARCHAR(256),
    sigleUniteLegale                          VARCHAR(256),
    sexeUniteLegale                           VARCHAR(256),
    prenom1UniteLegale                        VARCHAR(256),
    prenom2UniteLegale                        VARCHAR(256),
    prenom3UniteLegale                        VARCHAR(256),
    prenom4UniteLegale                        VARCHAR(256),
    prenomUsuelUniteLegale                    VARCHAR(256),
    pseudonymeUniteLegale                     VARCHAR(256),
    identifiantAssociationUniteLegale         VARCHAR(256),
    trancheEffectifsUniteLegale               VARCHAR(256),
    anneeEffectifsUniteLegale                 VARCHAR(256),
    dateDernierTraitementUniteLegale          VARCHAR(256),
    nombrePeriodesUniteLegale                 VARCHAR(256),
    categorieEntreprise                       VARCHAR(256),
    anneeCategorieEntreprise                  VARCHAR(256),
    dateDebut                                 VARCHAR(256),
    etatAdministratifUniteLegale              VARCHAR(256),
    nomUniteLegale                            VARCHAR(256),
    nomUsageUniteLegale                       VARCHAR(256),
    denominationUniteLegale                   VARCHAR(256),
    denominationUsuelle1UniteLegale           VARCHAR(256),
    denominationUsuelle2UniteLegale           VARCHAR(256),
    denominationUsuelle3UniteLegale           VARCHAR(256),
    categorieJuridiqueUniteLegale             VARCHAR(256),
    activitePrincipaleUniteLegale             VARCHAR(256),
    nomenclatureActivitePrincipaleUniteLegale VARCHAR(256),
    nicSiegeUniteLegale                       VARCHAR(256),
    economieSocialeSolidaireUniteLegale       VARCHAR(256),
    caractereEmployeurUniteLegale             VARCHAR(256)
);

DROP TABLE IF EXISTS {schemaint}.siren_present ;

CREATE TABLE IF NOT EXISTS {schemaint}.siren_present (LIKE {schemaint}.staging_siren);

CREATE TABLE IF NOT EXISTS {schemaint}.cpv (
    codecpv VARCHAR(8),
    description VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS {schemaint}.staging_infogreff (

)