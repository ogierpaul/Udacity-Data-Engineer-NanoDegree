CREATE TABLE siren_stock_raw
(
    siren                                       VARCHAR(256),
    nic                                         VARCHAR(256),
    siret                                       VARCHAR(256),
    dateFin                                     VARCHAR(256),
    dateDebut                                   VARCHAR(256),
    etatAdministratifEtablissement              VARCHAR(256),
    changementEtatAdministratifEtablissement    VARCHAR(256),
    enseigne1Etablissement                      VARCHAR(256),
    enseigne2Etablissement                      VARCHAR(256),
    enseigne3Etablissement                      VARCHAR(256),
    changementEnseigneEtablissement             VARCHAR(256),
    denominationUsuelleEtablissement            VARCHAR(256),
    changementDenominationUsuelleEtablissement  VARCHAR(256),
    activitePrincipaleEtablissement             VARCHAR(256),
    nomenclatureActivitePrincipaleEtablissement VARCHAR(256),
    changementActivitePrincipaleEtablissement   VARCHAR(256),
    caractereEmployeurEtablissement             VARCHAR(256),
    changementCaractereEmployeurEtablissement VARCHAR (256)
);

COPY siren_stock_raw FROM '/data/15-DECP/3-SIREN/StockEtablissementHistorique_utf8.csv' WITH (FORMAT CSV, QUOTE '"', delimiter ',');