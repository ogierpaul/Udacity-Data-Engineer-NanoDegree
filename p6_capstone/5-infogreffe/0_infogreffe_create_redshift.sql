DROP TABLE IF EXISTS {schemaint}.staging_infogreffe;

CREATE TABLE IF NOT EXISTS {schemaint}.staging_infogreffe
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
    num_dept                   VARCHAR(8),
    departement                VARCHAR(256),
    region                     VARCHAR(256),
    code_greffe                VARCHAR(256),
    greffe                     VARCHAR(256),
    date_immatriculation       VARCHAR(32),
    date_radiation             VARCHAR(32),
    statut                     VARCHAR(256),
    geolocalisation            VARCHAR(256),
    date_de_publication        VARCHAR(32),
    millesime_1                VARCHAR(64),
    date_de_cloture_exercice_1 VARCHAR(32),
    duree_1                    VARCHAR(16),
    ca_1                       VARCHAR(64),
    resultat_1                 VARCHAR(64),
    effectif_1                 VARCHAR(64),
    millesime_2                VARCHAR(64),
    date_de_cloture_exercice_2 VARCHAR(32),
    duree_2                    VARCHAR(16),
    ca_2                       VARCHAR(64),
    resultat_2                 VARCHAR(64),
    effectif_2                 VARCHAR(64),
    millesime_3                VARCHAR(64),
    date_de_cloture_exercice_3 VARCHAR(32),
    duree_3                    VARCHAR(16),
    ca_3                       VARCHAR(64),
    resultat_3                 VARCHAR(64),
    effectif_3                 VARCHAR(64),
    fiche_identite             VARCHAR(256),
    tranche_ca_millesime_1     VARCHAR(64),
    tranche_ca_millesime_2     VARCHAR(64),
    tranche_ca_millesime_3     VARCHAR(64)
);

DROP TABLE IF EXISTS {schemaout}.infogreffe;

CREATE TABLE IF EXISTS {schemaout}.infogreffe (LIKE {schemaint}.infogreffe);