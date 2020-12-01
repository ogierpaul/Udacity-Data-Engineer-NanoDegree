
COPY siren_stockunitelegale
FROM 's3://paulogiereucentral1/3-SIREN/StockUniteLegale_utf8.csv'
IAM_ROLE 'arn:aws:iam::075227836161:role/iacredshifts3access'
FORMAT AS CSV IGNOREHEADER AS 1 DELIMITER AS ',';


CREATE TABLE siren_stockunitelegale
(
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

)

SELECT
    *
FROM (SELECT DISTINCT siren
      FROM decp_suppliers
      WHERE siren IS NOT NULL
     )
LEFT JOIN
    siren_stockunitelegale
USING(siren)
LIMIT 5;

UNLOAD (
'SELECT
    *
FROM (SELECT DISTINCT siren
      FROM decp_supplier
      WHERE siren IS NOT NULL
     ) b
LEFT JOIN siren_stockunitelegale
USING(siren)')
TO 's3://paulogiereucentral1/siren_sample.csv'
IAM_ROLE 'arn:aws:iam::075227836161:role/iacredshifts3access'
FORMAT AS CSV HEADER DELIMITER AS '|'
PARALLEL OFF;