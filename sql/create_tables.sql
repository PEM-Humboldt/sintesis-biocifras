-- DDL de referencia para las tablas del proyecto sintesis-biocifras
-- Ejecutable directamente en PostgreSQL (psql, pgAdmin, DBeaver, etc.)
-- En el flujo normal, main.py crea las tablas con nombres fijos (dwc_occurrence, dwc_verbatim, dwc_integrated)

-- Tabla de registro de versiones de tablas

CREATE EXTENSION IF NOT EXISTS postgis;


CREATE TABLE IF NOT EXISTS public.gbif_publishers (
    publishingorgkey text PRIMARY KEY,
    organization text,
    institutionid text
);

CREATE TABLE IF NOT EXISTS public.gbif_datasets (
    datasetkey text PRIMARY KEY,
    license text,
    doi text,
    datasettitle text,
    logourl text,
    datatype text,
    created date
);



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

CREATE TABLE IF NOT EXISTS public.taxonomic_taxon_rank (
    id serial PRIMARY KEY,
    taxonrank text NOT NULL UNIQUE,
    taxonranktranslated text
);

INSERT INTO taxonomic_taxon_rank(taxonrank,taxonranktranslated)
VALUES
('KINGDOM','Reino'),
('PHYLUM','Filo'),
('CLASS','Clase'),
('ORDER','Orden'),
('FAMILY','Familia'),
('GENUS','Género'),
('SPECIES','Especie'),
('SUBSPECIES','Subespecie'),
('VARIETY','Variedad'),
('FORM','Forma'),
('UNRANKED',NULL) ON CONFLICT (taxon) DO UPDATE SET taxonranktranslated=EXCLUDED.taxonranktranslated;
