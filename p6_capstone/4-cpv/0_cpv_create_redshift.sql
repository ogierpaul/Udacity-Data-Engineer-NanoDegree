DROP TABLE IF EXISTS {schemaint}.cpv;

CREATE TABLE IF NOT EXISTS {schemaint}.cpv (
    codecpv VARCHAR(8) PRIMARY KEY,
    description VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS {schemaout}.cpv (
    codecpv VARCHAR(8) PRIMARY KEY,
    description VARCHAR(128)
);
