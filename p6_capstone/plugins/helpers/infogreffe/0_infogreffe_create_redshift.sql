CREATE TABLE IF NOT EXISTS staging.staging_infogreffe
(
    denomination               VARCHAR(256),
    siren                      VARCHAR(9),
    nic                        VARCHAR(6),
    forme_juridique            VARCHAR(32),
    code_ape                   VARCHAR(32),
    libelle_ape                VARCHAR(256),
    adresse                    VARCHAR(256),
    code_postal                VARCHAR(8),
    ville                      VARCHAR(256),
    date_de_cloture_exercice_1 VARCHAR(32),
    millesime_1                VARCHAR(64),
    tranche_ca_millesime_1     VARCHAR(64),
    duree_1                    VARCHAR(16),
    ca_1                       VARCHAR(64),
    resultat_1                 VARCHAR(64),
    effectif_1                 VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS staging.infogreffe
(
    siren                      VARCHAR(9) PRIMARY KEY,
    nic                        VARCHAR(6),
    denomination               VARCHAR(256),
    forme_juridique            VARCHAR(32),
    code_ape                   VARCHAR(32),
    libelle_ape                VARCHAR(256),
    adresse                    VARCHAR(256),
    code_postal                VARCHAR(8),
    ville                      VARCHAR(256),
    millesime               VARCHAR(64),
    date_de_cloture_exercice VARCHAR(32),
    duree                    VARCHAR(16),
    tranche_ca_millesime     VARCHAR(64),
    ca                       VARCHAR(64),
    resultat                 VARCHAR(64),
    effectif                 VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS reporting.infogreffe (LIKE staging.infogreffe);