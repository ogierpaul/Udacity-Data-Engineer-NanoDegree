TRUNCATE {schemaint}.siren_present;

INSERT INTO {schemaint}.siren_present
SELECT * FROM {schemaint}.staging_siren
INNER JOIN (SELECT DISTINCT siren from {schemaout}.decp_awarded WHERE NOT siren IS NULL) b USING (siren);

TRUNCATE {schemaout}.siren_directory;

INSERT INTO {schemaout}.siren_directory
SELECT
    siren,
    COALESCE(nomUniteLegale, '') || COALESCE(nomUsageUniteLegale, '') || COALESCE(denominationUniteLegale, '') || COALESCE(denominationUsuelle1UniteLegale, '') as name,
    economieSocialeSolidaireUniteLegale,
    trancheEffectifsUniteLegale,
    categorieentreprise
FROM
    {schemaint}.siren_present;

