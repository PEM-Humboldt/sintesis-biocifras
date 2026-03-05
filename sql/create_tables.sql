-- DDL de referencia para las tablas del proyecto sintesis-biocifras
-- Ejecutable directamente en PostgreSQL (psql, pgAdmin, DBeaver, etc.)
-- En el flujo normal, main.py crea las tablas con sufijo de fecha (ej: dwc_occurrence_20260304)

-- Tabla de registro de versiones de tablas
CREATE TABLE IF NOT EXISTS table_registry (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    created_at DATE NOT NULL,
    is_latest BOOLEAN NOT NULL DEFAULT TRUE
);

-- Tabla staging: occurrence (desde occurrence.txt de GBIF)
CREATE TABLE IF NOT EXISTS dwc_occurrence (
    "gbifID" BIGINT PRIMARY KEY,
    "occurrenceID" TEXT,
    "basisOfRecord" TEXT,
    "collectionCode" TEXT,
    "catalogNumber" TEXT,
    "recordedBy" TEXT,
    "individualCount" INTEGER,
    "eventDate" TEXT,
    "countryCode" TEXT,
    "stateProvince" TEXT,
    "locality" TEXT,
    "elevation" DOUBLE PRECISION,
    "depth" DOUBLE PRECISION,
    "decimalLatitude" DOUBLE PRECISION,
    "decimalLongitude" DOUBLE PRECISION,
    "coordinateUncertaintyInMeters" DOUBLE PRECISION,
    "scientificName" TEXT,
    "kingdom" TEXT,
    "phylum" TEXT,
    "class" TEXT,
    "order" TEXT,
    "family" TEXT,
    "genus" TEXT,
    "species" TEXT,
    "infraspecificEpithet" TEXT,
    "taxonRank" TEXT,
    "day" SMALLINT,
    "month" SMALLINT,
    "year" SMALLINT,
    "verbatimScientificName" TEXT,
    "datasetKey" TEXT,
    "publishingOrgKey" TEXT,
    "taxonKey" BIGINT,
    "issue" TEXT,
    "occurrenceStatus" TEXT,
    "lastInterpreted" TIMESTAMPTZ
);

-- Tabla staging: verbatim (desde verbatim.txt de GBIF)
CREATE TABLE IF NOT EXISTS dwc_verbatim (
    "gbifID" BIGINT PRIMARY KEY,
    "type" TEXT,
    "datasetID" TEXT,
    "datasetName" TEXT,
    "organismQuantity" TEXT,
    "organismQuantityType" TEXT,
    "eventID" TEXT,
    "samplingProtocol" TEXT,
    "county" TEXT,
    "municipality" TEXT,
    "repatriated" TEXT,
    "publishingCountry" TEXT,
    "lastParsed" TIMESTAMPTZ
);
