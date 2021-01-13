TRUNCATE staging.infogreffe;

INSERT INTO staging.infogreffe
SELECT
siren,
       nic,
       denomination,
       forme_juridique,
       code_ape,
       libelle_ape,
       adresse,
       code_postal,
       ville,
       millesime_1 AS millesime,
       date_de_cloture_exercice_1 AS date_cloture_exercice,
       duree_1 AS duree,
       tranche_ca_millesime_1 AS tranche_ca_millesime,
       ca_1 AS ca,
       resultat_1 AS resultat,
       effectif_1 AS effectif
FROM
     staging.staging_infogreffe
WHERE siren IS NOT NULL;

TRUNCATE reporting.infogreffe;

INSERT INTO reporting.infogreffe
SELECT * FROM staging.infogreffe
INNER JOIN (SELECT DISTINCT siren FROM staging.siren_directory) b
USING (siren);
