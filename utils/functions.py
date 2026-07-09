# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-03-04
"""
Este archivo contiene las funciones para la carga de datos desde GBIF a un servidor PostgreSQL + PostGIS
para el proceso de análisis y síntesis de cifras para Biodiversidad en cifras.
Rendimiento: FLUSH_EVERY controla el lote de COPY (SQL_BATCH_SIZE); UPDATE_BATCH_SIZE controla el LIMIT en UPDATE por lotes (JOIN/ctid).
- SIMPLE_COLS: Lista de columnas de la tabla dwc_occurrence (occurrence.txt / simple).
- OCURRENCE_COLS: Lista de columnas de la tabla dwc_verbatim (verbatim.txt).
- SQL_COLS: Lista de columnas de la tabla dwc_sql.
- register_load: Función para registrar la carga de datos en la tabla table_registry.
- tables_operations: Función para crear/truncar las tablas de staging (dwc_occurrence y dwc_verbatim) y la tabla integrada (dwc_integrated).
- data_upload: Función para cargar los datos desde los archivos TSV de GBIF a las tablas de staging (lote COPY vía FLUSH_EVERY / SQL_BATCH_SIZE).
- finalize_sql_table: Función para renombrar la columna v_scientificname y la tabla de staging dwc_sql a dwc_integrated.
- create_staging_indexes: Función para crear índices en las tablas de staging.
- create_integrated_table: Función para crear la tabla integrada con las columnas de las tablas de staging.
- fill_species_from_scientificname: Función para llenar el campo species con las dos primeras palabras de scientificname.
- normalize_integrated_country: Campo country desde countrycode (CO → Colombia; resto NULL) por lotes sobre gbifid.
- add_gbifid_index: Función para crear índice primary key sobre gbifid en la tabla integrada.
- create_species_index: Función para crear índice BTREE sobre species para optimizar cruces taxonómicos.
- validate_taxonomic_species: Crea taxonomic_species_validation (borrada en tables_operations) y catálogo por species.
- validate_localities: Crea geo_locality_validation (borrada en tables_operations) y catálogo por coordenadas/localidad.
- link_integrated_taxonomic_species_id: UPDATE por lotes (CTE) integrada → catálogo por species; índice parcial y VACUUM ANALYZE en integrada.
- link_integrated_locality_id: UPDATE por lotes (pending + JOIN 4 campos) integrada → geo_locality_validation; FK NOT VALID y VACUUM.
- spatials_joins: Cruza geo_locality_validation con MGN_ADM_MPIO_2025 y capas marítimas usando ST_Intersects.
- normalize_stateprovince_county: Normaliza stateprovince, county y slugs en geo_locality_validation antes de validar geografía.
- validate_geography: Valida geografía en geo_locality_validation (tres bloques con db.connect: depto, municipio, flaggeo).
- populate_geo_slugs: Persiste stateprovinceslug/countyslug en geo_locality_validation desde geo_master_geography.
- taxonomic_joins: Cruza taxonomic_species_validation con tablas taxonómicas por species.
- clean_threatstatus_fields: Normaliza threatstatus en taxonomic_species_validation (IUCN/MADS).
- gbif_api_calls: Completa gbif_datasets y gbif_publishers desde tablas locales y API GBIF; añade FK NOT VALID desde la integrada hacia esas tablas (validar aparte con VALIDATE CONSTRAINT).
- create_integrated_fk_indexes: Índices BTREE en datasetkey, publishingorgkey y taxonomic_species_id de la integrada.
"""

import csv
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import urllib.error
import urllib.request

# Inicialización del logger
logger = logging.getLogger('sintesis_biocifras')

# Tamaño de buffer para COPY (FLUSH_EVERY en .env; para reducir los round-trips al servidor se recomienda valores altos).
SQL_BATCH_SIZE = int(os.getenv('FLUSH_EVERY', '1000000'))
# Límite de filas por sentencia en UPDATE por lotes (JOIN + ctid).
UPDATE_BATCH_SIZE = int(os.getenv('UPDATE_BATCH_SIZE', '50000'))
# Parámetros de sesión usados en procesos de actualización.
_MAINTENANCE_WORK_MEM = os.getenv('MAINTENANCE_WORK_MEM', '2GB')
_WORK_MEM = os.getenv('WORK_MEM', '64MB')
# Workers paralelos para funciones de mantenimiento: CREATE INDEX / ADD PRIMARY KEY / VACUUM. 
# Dejar en 0 en WSL (Windows) / y en ambientes con Docker para evitar "could not resize shared memory segment".
# Sistemas operativos Linux se deja en 4 o con pruebas en valores mayores
_MAX_PARALLEL_MAINTENANCE_WORKERS = int(os.getenv('MAX_PARALLEL_MAINTENANCE_WORKERS', '4'))

DWC_OCCURRENCE_TABLE = 'dwc_occurrence'
DWC_VERBATIM_TABLE = 'dwc_verbatim'
DWC_SQL_TABLE = 'dwc_sql'
DWC_INTEGRATED_TABLE = 'dwc_integrated'


# ------------------------------------------------------------------------------------------------------------
# Definición de listas y variables para el proceso de carga desde los archivos TSV de GBIF
# 
# Para el process de carga desde los archivos integrated.csv, ocurrence.txt y sql.csv se definen únicamente 
# las columnas, con listas en python, que se van a utilizar para evitar cargar datos innecesarios y optimizar el 
# proceso de carga. Se pueden agregar más columnas si es necesario. Pero no olvidar agregar las columnas a las 
# tablas de staging en las listas _SIMPLE_TYPES, _OCURRENCE_TYPES, _SQL_COL_TYPES.
# Se decide usar este enfoque de listas para poder agregar o reducir el número de columnas de manera dinámica
# sin tener que modificar directamente consultas SQL en RAW.
# ------------------------------------------------------------------------------------------------------------

SIMPLE_COLS = [
    'gbifid', 'occurrenceid', 'basisofrecord', 'collectioncode',
    'catalognumber', 'recordedby', 'individualcount', 'eventdate',
    'countrycode', 'stateprovince', 'locality', 'elevation', 'depth',
    'decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters',
    'scientificname', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'infraspecificepithet', 'taxonrank', 'day', 'month',
    'year', 'verbatimscientificname', 'datasetkey', 'publishingorgkey',
    'taxonkey', 'issue', 'occurrencestatus', 'lastinterpreted',
]

OCURRENCE_COLS = [
    'gbifid', 'type', 'datasetid', 'datasetname', 'organismquantity',
    'organismquantitytype', 'eventid', 'samplingprotocol', 'county',
    'municipality', 'repatriated', 'publishingcountry', 'lastparsed',
]

SQL_COLS = [
    'gbifid', 'occurrenceid', 'basisofrecord',
    'collectioncode', 'catalognumber', 'recordedby', 'individualcount',
    'eventdate', 'countrycode', 'stateprovince', 'locality', 'elevation',
    'depth', 'decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters',
    'scientificname', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'infraspecificepithet', 'taxonrank', 'day', 'month',
    'year', 'v_scientificname', 'datasetkey', 'publishingorgkey', 'taxonkey', 'issue',
    'occurrencestatus', 'lastinterpreted', 'type', 'datasetid', 'datasetname',
    'organismquantity', 'organismquantitytype', 'eventid', 'samplingprotocol',
    'county', 'municipality', 'repatriated', 'publishingcountry', 'lastparsed',
]

# Mapeo de columnas tipo SQL para CREATE TABLE dinámico
_SIMPLE_TYPES = {
    'gbifid': 'BIGINT',
    'occurrenceid': 'TEXT', 'basisofrecord': 'TEXT',
    'collectioncode': 'TEXT', 'catalognumber': 'TEXT',
    'recordedby': 'TEXT', 'individualcount': 'INTEGER',
    'eventdate': 'TEXT', 'countrycode': 'TEXT',
    'stateprovince': 'TEXT', 'locality': 'TEXT',
    'elevation': 'DOUBLE PRECISION', 'depth': 'DOUBLE PRECISION',
    'decimallatitude': 'DOUBLE PRECISION',
    'decimallongitude': 'DOUBLE PRECISION',
    'coordinateuncertaintyinmeters': 'DOUBLE PRECISION',
    'scientificname': 'TEXT', 'kingdom': 'TEXT', 'phylum': 'TEXT',
    'class': 'TEXT', 'order': 'TEXT', 'family': 'TEXT',
    'genus': 'TEXT', 'species': 'TEXT', 'infraspecificepithet': 'TEXT',
    'taxonrank': 'TEXT', 'day': 'SMALLINT', 'month': 'SMALLINT',
    'year': 'SMALLINT', 'verbatimscientificname': 'TEXT',
    'datasetkey': 'TEXT', 'publishingorgkey': 'TEXT',
    'taxonkey': 'BIGINT', 'issue': 'TEXT', 'occurrencestatus': 'TEXT',
    'lastinterpreted': 'TIMESTAMPTZ',
}

_OCURRENCE_TYPES = {
    'gbifid': 'BIGINT',
    'type': 'TEXT', 'datasetid': 'TEXT', 'datasetname': 'TEXT',
    'organismquantity': 'TEXT', 'organismquantitytype': 'TEXT',
    'eventid': 'TEXT', 'samplingprotocol': 'TEXT',
    'county': 'TEXT', 'municipality': 'TEXT',
    'repatriated': 'TEXT', 'publishingcountry': 'TEXT',
    'lastparsed': 'TIMESTAMPTZ',
}

_SQL_COL_TYPES = {
    'gbifid': 'BIGINT',
    'occurrenceid': 'TEXT',
    'basisofrecord': 'TEXT', 'collectioncode': 'TEXT',
    'catalognumber': 'TEXT', 'recordedby': 'TEXT',
    'individualcount': 'INTEGER', 'eventdate': 'TEXT',
    'countrycode': 'TEXT', 'stateprovince': 'TEXT',
    'locality': 'TEXT', 'elevation': 'DOUBLE PRECISION',
    'depth': 'DOUBLE PRECISION',
    'decimallatitude': 'DOUBLE PRECISION',
    'decimallongitude': 'DOUBLE PRECISION',
    'coordinateuncertaintyinmeters': 'DOUBLE PRECISION',
    'scientificname': 'TEXT', 'kingdom': 'TEXT', 'phylum': 'TEXT',
    'class': 'TEXT', 'order': 'TEXT', 'family': 'TEXT',
    'genus': 'TEXT', 'species': 'TEXT', 'infraspecificepithet': 'TEXT',
    'taxonrank': 'TEXT', 'day': 'SMALLINT', 'month': 'SMALLINT',
    'year': 'SMALLINT', 'v_scientificname': 'TEXT', 'datasetkey': 'TEXT',
    'publishingorgkey': 'TEXT', 'taxonkey': 'BIGINT',
    'issue': 'TEXT', 'occurrencestatus': 'TEXT',
    'type': 'TEXT', 'datasetid': 'TEXT', 'datasetname': 'TEXT',
    'organismquantity': 'TEXT', 'organismquantitytype': 'TEXT',
    'eventid': 'TEXT', 'samplingprotocol': 'TEXT',
    'county': 'TEXT', 'municipality': 'TEXT',
    'repatriated': 'BOOLEAN', 'publishingcountry': 'TEXT',
    'lastinterpreted': 'TIMESTAMPTZ', 'lastparsed': 'TIMESTAMPTZ',
}

# -----------------------------------------------------------------------------------------------------
# Mantenimiento posterior a actualizaciones masivas, función helper
# -----------------------------------------------------------------------------------------------------

def _run_table_maintenance(db, table_name):
    # Mantenimiento post-actualización masiva. Hace un vacuum_analyze por defecto, con variable en el .env
    # Tener en cuenta que un vacumm full necesita de almacenamiento, al menos igual al valor de la tablas
    if os.getenv('SKIP_TABLE_MAINTENANCE', 'false').lower() == 'true':
        logger.info("Mantenimiento omitido (SKIP_TABLE_MAINTENANCE=true): %s", table_name)
        return

    mode = os.getenv('TABLE_MAINTENANCE_MODE', 'vacuum_analyze').lower()
    vacuum_mem = os.getenv('VACUUM_MAINTENANCE_WORK_MEM', '512MB')
    raw_conn = db.raw_connection()
    try:
        raw_conn.autocommit = True
        with raw_conn.cursor() as cur:
            cur.execute(f'SET max_parallel_maintenance_workers = {_MAX_PARALLEL_MAINTENANCE_WORKERS}')
            cur.execute(f"SET maintenance_work_mem = '{vacuum_mem}'")
            if mode == 'analyze':
                cur.execute(f'ANALYZE "{table_name}"')
                logger.info("ANALYZE completado en %s", table_name)
            else:
                cur.execute(f'VACUUM (ANALYZE) "{table_name}"')
                logger.info("VACUUM (ANALYZE) completado en %s", table_name)
                if os.getenv('RUN_VACUUM_FULL', 'false').lower() == 'true':
                    cur.execute(f'VACUUM (FULL, ANALYZE) "{table_name}"')
                    logger.info("VACUUM (FULL, ANALYZE) completado en %s", table_name)
    finally:
        raw_conn.close()

# -------------------------------------------------------------------------------------------------------------------------
# Creacion / truncado de tablas de staging (integrates y ocurrence) y la tabla integrada (dwc_integrated)
# -------------------------------------------------------------------------------------------------------------------------

def _build_create_ddl(table_name, col_types):
    # Función de apoyo.
    # Genera sentencias CREATE TABLE a partir del diccionario columna -> tipo SQL.
    # cols es un diccionario con el nombre de la columna y el tipo SQL que se genera dinámicamente
    # col_types es uno de los diccionarios definidos al inicio de este archivo: _SIMPLE_TYPES, _OCURRENCE_TYPES, _SQL_COL_TYPES
    # Es equivalente a ejecutar la siguiente consulta:
    # CREATE UNLOGGED TABLE "tabla_fecha" (...) WITH (autovacuum_enabled = false);
    # El autovacuum está desactivado para evitar procesos en paralelo, por lo que luego de cada función de carga/actualización
    # masiva se ejecuta un vacuum_analyze o full (si está configurado en el .env)
    # Parámetros:
    # - table_name: Nombre de la tabla a crear.
    # - col_types: Diccionario con los tipos de columnas para la tabla: _SIMPLE_TYPES, _OCURRENCE_TYPES, _SQL_COL_TYPES
    # Retorna:
    # - ddl: Sentencia CREATE TABLE para la tabla.
    cols = ', '.join(f'"{col}" {dtype}' for col, dtype in col_types.items())
    return f"""
        CREATE UNLOGGED TABLE "{table_name}" ({cols})
        WITH (autovacuum_enabled = false);
    """

def tables_operations(db, upload_type=None):
    # Crea tablas de staging e integrada con nombres fijos. Si ya existen, las elimina y vuelven a crear.
    # Recrea staging, elimina la integrada y luego las tablas de validación.
    # Se tienen el cuenta el tipo de carga: sql o regular.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - upload_type: Tipo de carga: sql o regular.
    # Retorna:
    # - table_names: Diccionario con los nombres de las tablas de staging y la tabla integrada.
    # - type_maps: Diccionario con los tipos de columnas para las tablas de staging y la tabla integrada.
    # - keys: Tupla con los nombres de las tablas de staging y la tabla integrada.
    if upload_type not in {"sql", "regular"}:
        raise ValueError(
            f"upload_type inválido: {upload_type}. Debe ser 'sql' o 'regular'."
        )

    if upload_type == "sql":
        table_names = {'sql': DWC_SQL_TABLE, 'integrated': DWC_INTEGRATED_TABLE}
        type_maps = {'sql': _SQL_COL_TYPES}
        keys = ('sql',)
    else:
        table_names = {
            'occurrence': DWC_OCCURRENCE_TABLE,
            'verbatim': DWC_VERBATIM_TABLE,
            'integrated': DWC_INTEGRATED_TABLE,
        }
        type_maps = {
            'occurrence': _SIMPLE_TYPES,
            'verbatim': _OCURRENCE_TYPES,
        }
        keys = ('occurrence', 'verbatim')
    validation_tables = ('taxonomic_species_validation', 'geo_locality_validation')
    if True:
        with db.connect() as conn:
            logger.info("Ejecutando consulta")
            conn.execute(f'DROP TABLE IF EXISTS "{DWC_INTEGRATED_TABLE}" CASCADE')
            conn.commit()
            logger.info("DROP TABLE %s", DWC_INTEGRATED_TABLE)
            for key in keys:
                tname = table_names[key]
                logger.info("Ejecutando consulta")
                conn.execute(f'DROP TABLE IF EXISTS "{tname}" CASCADE')
                conn.commit()
                logger.info("DROP TABLE %s", tname)
                ddl = _build_create_ddl(tname, type_maps[key])
                logger.info("Ejecutando consulta")
                conn.execute(ddl)
                conn.commit()
                logger.info("CREATE TABLE %s", tname)
            for vtbl in validation_tables:
                logger.info("Ejecutando consulta")
                conn.execute(f'DROP TABLE IF EXISTS "{vtbl}"')
                conn.commit()
                logger.info("DROP TABLE %s", vtbl)
    return table_names

# -------------------------------------------------------------------------------------------------------------------------
# Actualización tabla de registro
# -------------------------------------------------------------------------------------------------------------------------

def register_load(db, table_names, created_at, origin):
    # Registra la carga en table_registry (created_at = fecha de ejecución; nombres de tabla fijos).
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_names: Diccionario con los nombres de las tablas de staging y la tabla integrada.
    # - created_at: Fecha de la carga registrada en table_registry.
    # - origin: Origen de la carga: SQL o DwC-A.
    # Retorna:
    # - None: No retorna nada.
    with db.connect() as conn:
        for table_name in table_names.values():
            logger.info("Ejecutando consulta")
            conn.execute(
                "UPDATE table_registry SET is_latest = FALSE "
                "WHERE table_name = %(table_name)s AND is_latest = TRUE",
                {'table_name': table_name},
            )
            conn.commit()
            logger.info("Ejecutando consulta")
            conn.execute(
                "INSERT INTO table_registry (table_name, origin, created_at, is_latest) "
                "VALUES (%(table_name)s, %(origin)s, %(created_at)s, TRUE)",
                {'table_name': table_name, 'origin': origin, 'created_at': created_at},
            )
        conn.commit()
    logger.info("Datos cargados en table_registry.")


# ------------------------------------------------------------------------------------------------------------
# Carga masiva de datos desde los archivos TSV de GBIF a las tablas de staging
# ------------------------------------------------------------------------------------------------------------

# - psycopg2.copy_expert: permite COPY FROM STDIN por buffer (sin requerir el archivo en la misma base de datos)
#   y deja control sobre el tamaño de lote (flush_size). Combinado con csv.writer maneja comillas/tabs automáticamente.
# - _EPOCH_MS_COLS: columnas TIMESTAMPTZ que GBIF entrega en epoch ms; se convierten a ISO 8601 antes del COPY para que PostgreSQL pueda entenderlas.
_EPOCH_MS_COLS = {'lastinterpreted', 'lastparsed'}

def _epoch_ms_to_iso(value):
    # Convierte epoch en milisegundos a ISO 8601 para columnas TIMESTAMPTZ.
    if not value:
        return value
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return value


def data_upload(db, filepath, table_name, columns):
    """Carga desde el archivo definido en `filepath` y carga a la tabla de acuerdo al tipo de carga por COPY en lotes según la variable SQL_BATCH_SIZE filas.

    Si `filepath` no está definido o no existe, lanza FileNotFoundError (sin actualizar la tabla `tables_operations`).
    """
    if not filepath or not Path(filepath).is_file():
        msg = (
            f"No se definió la ruta del archivo en el .env para la tabla {table_name}"
            if not filepath
            else f"El archivo no existe en la ruta indicada en el .env: {filepath}"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    quoted_cols = ', '.join(f'"{c.lower()}"' for c in columns)
    copy_sql = f"""
        COPY "{table_name}" ({quoted_cols})
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')
    """

    flush_size = SQL_BATCH_SIZE
    with closing(db.raw_connection()) as raw_conn, raw_conn.cursor() as cur:
        try:
            cur.execute("SET synchronous_commit = OFF")
            cur.execute(f"SET maintenance_work_mem = '{_MAINTENANCE_WORK_MEM}'")
            cur.execute(f"SET work_mem = '{_WORK_MEM}'")
            buffer = io.StringIO()
            writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            count = 0
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
                header = next(reader, None)
                if not header:
                    raise ValueError(f"Archivo vacío o sin encabezado: {filepath}")
                header_map = {name.lower(): idx for idx, name in enumerate(header)}
                col_specs = [
                    (header_map.get(c.lower()), c.lower() in _EPOCH_MS_COLS)
                    for c in columns
                ]

                for row in reader:
                    writer.writerow([
                        _epoch_ms_to_iso(row[idx]) if is_epoch and idx is not None and idx < len(row)
                        else (row[idx] if idx is not None and idx < len(row) else '')
                        for idx, is_epoch in col_specs
                    ])
                    count += 1
                    if count % flush_size == 0:
                        buffer.seek(0)
                        cur.copy_expert(copy_sql, buffer)
                        raw_conn.commit()
                        buffer = io.StringIO()
                        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                        logger.info("  %s — %s filas cargadas...", table_name, f"{count:,}")
            if buffer.tell() > 0:
                buffer.seek(0)
                cur.copy_expert(copy_sql, buffer)
                raw_conn.commit()

            logger.info("  %s — carga completa: %s filas totales.", table_name, f"{count:,}")
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            cur.execute("RESET synchronous_commit")
            cur.execute("RESET maintenance_work_mem")
            cur.execute("RESET work_mem")


# -----------------------------------------------------------------------------------------------------
# Operaciones sobre la tabla de staging dwc_sql
# -----------------------------------------------------------------------------------------------------

def finalize_sql_table(db, old_name, new_name):
    """Renombra la columna v_scientificname → verbatimscientificname, renombra `old_name`
    a `new_name` y desactiva autovacuum en la tabla resultante."""
    with db.connect() as conn:
        conn.execute(
            f'ALTER TABLE "{old_name}" '
            f'RENAME COLUMN "v_scientificname" TO "verbatimscientificname"'
        )
        conn.execute(f'DROP TABLE IF EXISTS "{new_name}"')
        conn.execute(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"')
        conn.execute(f'ALTER TABLE "{new_name}" SET (autovacuum_enabled = false)')
        conn.commit()
    logger.info("Tabla SQL finalizada: %s → %s", old_name, new_name)

# -----------------------------------------------------------------------------------------------------
# Creación de índices en las tablas de staging dwc_occurrence y dwc_verbatim
# -----------------------------------------------------------------------------------------------------

def create_staging_indexes(db, table_names):
    """Crea índice BTREE en el campo gbifid para `occurrence` y `verbatim`."""
    with db.connect() as conn:
        conn.execute(f"SET maintenance_work_mem = '{_MAINTENANCE_WORK_MEM}'")
        conn.execute(f"SET max_parallel_maintenance_workers = {_MAX_PARALLEL_MAINTENANCE_WORKERS}")
        for key in ('occurrence', 'verbatim'):
            tname = table_names[key]
            idx_name = f"idx_{tname}_gbifid"
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{tname}" ("gbifid")')
            conn.commit()
            logger.info("Indice creado: %s", idx_name)


# -----------------------------------------------------------------------------------------------------
# Creación de la tabla integrada dwc_occurrence_integrated desde las tablas de staging 
# -----------------------------------------------------------------------------------------------------

def create_integrated_table(db, table_names):
    """Crea `integrated` con JOIN de `occurrence` y `verbatim` por gbifid.

    Se hace un ANALYZE previo para que el planner de postgres tenga estadísticas actualizadas tras el COPY.
    """
    occurrence = table_names['occurrence']
    verbatim = table_names['verbatim']
    integrated = table_names['integrated']

    occurrence_cols = ', '.join(f'o."{c.lower()}"' for c in SIMPLE_COLS)
    verbatim_cols = ', '.join(f'v."{c.lower()}"' for c in OCURRENCE_COLS if c != 'gbifid')

    with db.connect() as conn:
        conn.execute(f"SET work_mem = '{_WORK_MEM}'")
        conn.execute(f"SET maintenance_work_mem = '{_MAINTENANCE_WORK_MEM}'")
        conn.execute("SET max_parallel_workers_per_gather = 4")
        conn.execute(f'ANALYZE "{occurrence}"')
        conn.execute(f'ANALYZE "{verbatim}"')
        conn.execute(f'DROP TABLE IF EXISTS "{integrated}"')
        conn.execute(f"""
            CREATE TABLE "{integrated}" WITH (autovacuum_enabled = false) AS
            SELECT {occurrence_cols}, {verbatim_cols}
            FROM "{occurrence}" o
            INNER JOIN "{verbatim}" v ON o."gbifid" = v."gbifid"
        """)
        conn.commit()
    logger.info("Tabla integrada creada: %s", integrated)

# -----------------------------------------------------------------------------------------------------
# Revisión de casos de nombres científicos vacíos en la tabla integrada
# -----------------------------------------------------------------------------------------------------

def fill_species_from_scientificname(db, table_name):
    """Rellena species con las dos primeras palabras de scientificname cuando el campo taxonrank ='SPECIES'
    y el campo species está vacío."""
    with db.connect() as conn:
        result = conn.execute(f"""
            UPDATE "{table_name}"
            SET "species" = TRIM(
                split_part("scientificname", ' ', 1) || ' ' ||
                split_part("scientificname", ' ', 2)
            )
            WHERE "taxonrank" = 'SPECIES'
              AND "scientificname" IS NOT NULL
              AND TRIM("scientificname") <> ''
              AND ("species" IS NULL OR TRIM("species") = '')
        """)
        conn.commit()
    logger.info(
        "Campo species completado desde scientificname en %s (%s filas)",
        table_name, f"{result.rowcount:,}",
    )


# -----------------------------------------------------------------------------------------------------
# Nombre de país Colombia en integrada (DwC country)
# -----------------------------------------------------------------------------------------------------

def normalize_integrated_country(db, table_name):
    """Añade la columna `country` con DEFAULT 'Colombia'. Se puede usar este enfoque ya que
    todo registro descargado de GBIF tiene condición countrycode='CO'."""
    with db.connect() as conn:
        conn.execute(
            f'ALTER TABLE "{table_name}" '
            f"ADD COLUMN IF NOT EXISTS \"country\" TEXT DEFAULT 'Colombia'"
        )
        conn.commit()
    logger.info("Columna country añadida con DEFAULT 'Colombia' en %s", table_name)


# -----------------------------------------------------------------------------------------------------
# Índices BTREE en columnas FK de la integrada (dataset, publisher, especie)
# -----------------------------------------------------------------------------------------------------

def create_integrated_fk_indexes(db, table_name):
    """Crea índices BTREE en columnas con FK hacia catálogos GBIF y taxonomic_species_validation."""
    integrated = table_name
    fk_columns = (
        ('datasetkey', f'idx_{integrated}_datasetkey'),
        ('publishingorgkey', f'idx_{integrated}_publishingorgkey'),
        ('taxonomic_species_id', f'idx_{integrated}_taxonomic_species_id'),
    )
    with db.connect() as conn:
        for column, idx_name in fk_columns:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                f'ON "{integrated}" USING BTREE ("{column}")'
            )
            conn.commit()
            logger.info('Indice BTREE creado: %s (%s)', idx_name, column)


# -----------------------------------------------------------------------------------------------------
# Vinculación de taxonrank con la tabla catálogo taxonomic_taxon_rank y creación de integridad referencial
# -----------------------------------------------------------------------------------------------------

def link_taxonrank_reference(db, table_name):
    # Vincula taxonrank con la tabla catálogo taxonomic_taxon_rank y crea integridad referencial.
    # El proceso se ejecuta por lotes para evitar locks largos y picos de WAL en tablas grandes.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    integrated = table_name
    tmp_idx_integrated = f"idx_tmp_{integrated}_taxonrank"
    fk_name = f"fk_{integrated}_taxonrank_id"

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "taxonrank_id" INTEGER'
        )
        conn.commit()
        logger.info("Columna taxonrank_id creada en %s", integrated)

        # Índice temporal de apoyo para acelerar el join por taxonrank.
        logger.info("Ejecutando consulta")
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{tmp_idx_integrated}" '
            f'ON "{integrated}" USING BTREE ("taxonrank")'
        )
        conn.commit()
        logger.info("Indice temporal creado: %s", tmp_idx_integrated)

        # Actualización en una sola pasada: suele escalar mejor que re-escanear por lotes
        # cuando el catálogo de referencia (taxonomic_taxon_rank) es pequeño.
        logger.info("Ejecutando consulta")
        result = conn.execute(f"""
            UPDATE "{integrated}" i
            SET "taxonrank_id" = t."id"
            FROM "taxonomic_taxon_rank" t
            WHERE i."taxonrank" = t."taxonrank"
              AND i."taxonrank_id" IS NULL
              AND i."taxonrank" IS NOT NULL
        """)
        conn.commit()
        total_updated = result.rowcount
        conn.commit()
        logger.info(
            "Vinculación de taxonrank en %s: %s filas actualizadas",
            integrated,
            f"{total_updated:,}",
        )

        # Índice final para consultas por llave foránea.
        logger.info("Ejecutando consulta")
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{integrated}_taxonrank_id" '
            f'ON "{integrated}" USING BTREE ("taxonrank_id")'
        )
        conn.commit()
        logger.info("Indice creado en %s: taxonrank_id", integrated)

        # FK nullable: permite huérfanos (taxonrank_id = NULL) cuando no hay match en catálogo.
        logger.info("Ejecutando consulta")
        conn.execute(f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{fk_name}"')
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f"""
            ALTER TABLE "{integrated}"
            ADD CONSTRAINT "{fk_name}"
            FOREIGN KEY ("taxonrank_id")
            REFERENCES "taxonomic_taxon_rank" ("id")
            ON UPDATE CASCADE
            ON DELETE SET NULL
            NOT VALID
            """
        )
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(f'ALTER TABLE "{integrated}" VALIDATE CONSTRAINT "{fk_name}"')
        conn.commit()
        logger.info("Integridad referencial creada: %s", fk_name)

        # Limpieza de índice temporal.
        logger.info("Ejecutando consulta")
        conn.execute(f'DROP INDEX IF EXISTS "{tmp_idx_integrated}"')
        conn.commit()
        logger.info("Indice temporal eliminado: %s", tmp_idx_integrated)

    logger.info(
        "Vinculación de taxonrank completada en %s (%s filas con taxonrank_id)",
        integrated,
        f"{total_updated:,}",
    )


# -----------------------------------------------------------------------------------------------------
# Creación de indice primary key
# -----------------------------------------------------------------------------------------------------

def add_gbifid_index(db, table_name):
    """Añade PRIMARY KEY sobre gbifid en la tabla integrada."""
    integrated = table_name
    pk_name = f"pk_{integrated}_gbifid"
    with db.connect() as conn:
        conn.execute(f"SET maintenance_work_mem = '{_MAINTENANCE_WORK_MEM}'")
        conn.execute(f"SET max_parallel_maintenance_workers = {_MAX_PARALLEL_MAINTENANCE_WORKERS}")
        conn.execute(f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{pk_name}"')
        conn.execute(f'ALTER TABLE "{integrated}" ADD CONSTRAINT "{pk_name}" PRIMARY KEY ("gbifid")')
        conn.commit()
    logger.info("PK %s agregada en %s", pk_name, integrated)

# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces espaciales con la tabla MGN_ADM_MPIO_2025 (división político-administrativa) e Invemar_maritime_regions (regiones marítimas)
# --------------------------------------------------------------------------------------------------------------------------------------

# Palabras que se deben convertir a minúsculas después de INITCAP en los campos de departamento y municipio
# para estandarización de nombres. Por ejemplo, 'Norte De Santander' a 'Norte de Santander'.
_LOWERCASE_WORDS = (' De ', ' Y ', ' Del ', ' La ')

def _run_spatial_join(conn, set_clause, src_table, where_extra, log_label):
    """Helper: corre un UPDATE espacial por lotes sobre geo_locality_validation.
    - set_clause: SET de la sentencia
    - src_table: tabla externa con la geometría a intersectar (MGN_ADM_MPIO_2025, INVEMAR_MARITIME_REGIONS, NARINO_MARITIME_REGION).
    - where_extra: WHERE adicional para el batch (debe arrancar con AND).
    - log_label: etiqueta corta para el log de progreso.
    """
    locality_tbl = 'geo_locality_validation'
    batch_size = UPDATE_BATCH_SIZE
    total = 0
    while True:
        result = conn.execute(f"""
            WITH batch AS (
                SELECT ctid
                FROM "{locality_tbl}"
                WHERE geom IS NOT NULL
                  {where_extra}
                LIMIT {batch_size}
            )
            UPDATE "{locality_tbl}" i
            SET {set_clause}
            FROM batch b, "{src_table}" m
            WHERE i.ctid = b.ctid
              AND ST_Intersects(i.geom, m.geom)
        """)
        conn.commit()
        n = result.rowcount
        if n == 0:
            break
        total += n
        logger.info("Cruce %s batch: %s filas (total %s)", log_label, f"{n:,}", f"{total:,}")
    logger.info("Cruce espacial con %s completado (%s filas)", src_table, f"{total:,}")
    return total


def spatials_joins(db, table_name):
    """Cruza geo_locality_validation con MGN/INVEMAR/NARINO vía ST_Intersects y aplica
    INITCAP con normalizaciones (` De ` → ` de `, etc.) sobre depto/municipio."""
    _ = table_name #
    locality_tbl = 'geo_locality_validation'
    with db.connect() as conn:
        conn.execute(f"SET work_mem = '{_WORK_MEM}'")
        conn.execute("SET max_parallel_workers_per_gather = 4")

        _run_spatial_join(
            conn,
            set_clause='"stateprovincemgn" = m."dpto_cnmbr", "countymgn" = m."mpio_cnmbr"',
            src_table='MGN_ADM_MPIO_2025',
            where_extra='AND "stateprovincemgn" IS NULL',
            log_label='MGN',
        )
        _run_spatial_join(
            conn,
            set_clause='"maritimeregion" = m."DESCRIP"',
            src_table='INVEMAR_MARITIME_REGIONS',
            where_extra='AND "countymgn" IS NULL AND "maritimeregion" IS NULL',
            log_label='INVEMAR',
        )
        _run_spatial_join(
            conn,
            set_clause='"narinomaritimeregion" = m."Nombre"',
            src_table='NARINO_MARITIME_REGION',
            where_extra='AND "narinomaritimeregion" IS NULL',
            log_label='Nariño',
        )

        # INITCAP + normalizaciones (' De ' → ' de ', etc.) para estandarizar nombres.
        for col in ('stateprovincemgn', 'countymgn'):
            expr = f'INITCAP("{col}")'
            for word in _LOWERCASE_WORDS:
                expr = f"REPLACE({expr}, '{word}', '{word.lower()}')"
            conn.execute(f"""
                UPDATE "{locality_tbl}"
                SET "{col}" = {expr}
                WHERE "{col}" IS NOT NULL
                  AND "{col}" IS DISTINCT FROM {expr}
            """)
            conn.commit()
            logger.info("INITCAP normalizado en %s.%s", locality_tbl, col)

        # Bogotá, D.C. → Bogotá, D. C. (consistencia con salida de síntesis).
        conn.execute(f"""
            UPDATE "{locality_tbl}"
            SET "stateprovincemgn" = 'Bogotá, D. C.',
                "countymgn"        = 'Bogotá, D. C.'
            WHERE "stateprovincemgn" = 'Bogotá, D.C.'
        """)
        conn.commit()
        logger.info("Reemplazo manual de Bogotá D.C. aplicado en %s", locality_tbl)

# -----------------------------------------------------------------------------------------------------
# Normalización de stateprovince y county en geo_locality_validation
# -----------------------------------------------------------------------------------------------------

def _locality_queries_helper(conn, sql, label):
    """Helper: ejecuta `sql` (UPDATE por lotes sobre geo_locality_validation) hasta rowcount==0.

    El SQL debe incluir un WHERE que excluya filas ya procesadas
    (`WHERE ... IS NULL`)."""
    total = 0
    while True:
        n = conn.execute(sql).rowcount
        conn.commit()
        if n == 0:
            break
        total += n
        logger.info("%s batch: %s filas (total %s)", label, f"{n:,}", f"{total:,}")
    logger.info("%s completado (%s filas)", label, f"{total:,}")
    return total


def normalize_stateprovince_county(db, table_name):
    """Normaliza stateprovince/county en geo_locality_validation antes de validar geografía
    y luego asigna geo_master_geography_id desde DIVIPOLA. Todo por lotes"""
    _ = table_name  # firma mantenida para compatibilidad con el orquestador.
    locality = 'geo_locality_validation'
    batch_size = UPDATE_BATCH_SIZE

    # Reusado por las validaciones 3a y 3b del municipio.
    invalid_county_pair = """
        NULLIF(BTRIM(i."countyvalidated"), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM "geo_master_geography" m
            INNER JOIN "geo_master_geography" d ON d."id" = m."parent_id"
            WHERE m."subtype" = 'municipio'
              AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countyvalidated"))
              AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated"))
        )
    """

    with db.connect() as conn:
        # Índice de soporte para el NOT EXISTS de la validación 3 y los cruces DIVIPOLA.
        conn.execute("""
            CREATE INDEX IF NOT EXISTS "idx_geo_master_geography_subtype_name_upper"
            ON "geo_master_geography" ("subtype", UPPER(TRIM("name")), "parent_id")
        """)
        conn.commit()

        # Estadísticas actualizadastras spatials_joins para que el planner.
        conn.execute(f'ANALYZE "{locality}"')
        conn.execute(f"SET work_mem = '{_WORK_MEM}'")
        conn.execute("SET max_parallel_workers_per_gather = 4")

        # Validación 1 (departamento): catálogo de alias → stateprovincevalidated.
        _locality_queries_helper(conn, f"""
            WITH candidates AS (
                SELECT DISTINCT i.ctid
                FROM "{locality}" i
                INNER JOIN "geo_stateprovince_validation" a
                  ON UPPER(TRIM(i."stateprovince")) = UPPER(TRIM(a."originalstateprovince"))
                INNER JOIN "geo_master_geography" d ON d."id" = a."geo_master_geography_id"
                WHERE i."stateprovincevalidated" IS NULL
                  AND a."geo_master_geography_id" IS NOT NULL
            ), batch AS (
                SELECT ctid FROM candidates LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "stateprovincevalidated" = TRIM(d."name")
            FROM batch b, "geo_stateprovince_validation" a, "geo_master_geography" d
            WHERE i.ctid = b.ctid
              AND UPPER(TRIM(i."stateprovince")) = UPPER(TRIM(a."originalstateprovince"))
              AND d."id" = a."geo_master_geography_id"
        """, label="Validación 1 (alias departamento)")

        # Validación 2 (departamento): región marina Nariño  (Afluvial) se asigna a todo nariño 'Nariño'.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT ctid
                FROM "{locality}"
                WHERE BTRIM(COALESCE("narinomaritimeregion", '')) <> ''
                  AND "stateprovincevalidated" IS DISTINCT FROM 'Nariño'
                LIMIT {batch_size}
            )
            UPDATE "{locality}" t
            SET "stateprovincevalidated" = 'Nariño'
            FROM batch b
            WHERE t.ctid = b.ctid
        """, label="Validación 2 (Nariño marítimo)")

        # Validación 3 (departamento): copia MGN sólo cuando falta stateprovincevalidated.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT ctid
                FROM "{locality}"
                WHERE "stateprovincemgn" IS NOT NULL
                  AND "stateprovincevalidated" IS NULL
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "stateprovincevalidated" = TRIM(i."stateprovincemgn")
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Validación 3 (MGN departamento)")

        # Validación 1 (municipio): catálogo de alias → countyvalidated.
        _locality_queries_helper(conn, f"""
            WITH candidates AS (
                SELECT DISTINCT i.ctid
                FROM "{locality}" i
                INNER JOIN "geo_county_validation" c
                  ON UPPER(TRIM(i."county")) = UPPER(TRIM(c."originalcounty"))
                WHERE i."countyvalidated" IS NULL
                  AND i."county" IS NOT NULL
                  AND c."revisedcounty" IS NOT NULL
            ), batch AS (
                SELECT ctid FROM candidates LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "countyvalidated" = TRIM(c."revisedcounty")
            FROM batch b, "geo_county_validation" c
            WHERE i.ctid = b.ctid
              AND UPPER(TRIM(i."county")) = UPPER(TRIM(c."originalcounty"))
        """, label="Validación 1 (alias municipio)")

        # Validación 2 (municipio): copia MGN cuando countyvalidated está vacío.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT ctid
                FROM "{locality}"
                WHERE "countymgn" IS NOT NULL
                  AND "countyvalidated" IS NULL
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "countyvalidated" = TRIM(i."countymgn")
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Validación 2 (MGN municipio)")

        # Validación 3a (municipio): sustituir countyvalidated por countymgn cuando la pareja
        # (depto validado, countymgn) es válida en DIVIPOLA y la actual no.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT i.ctid
                FROM "{locality}" i
                INNER JOIN "geo_master_geography" m
                  ON m."subtype" = 'municipio'
                 AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countymgn"))
                INNER JOIN "geo_master_geography" d
                  ON d."id" = m."parent_id"
                 AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated"))
                WHERE {invalid_county_pair}
                  AND NULLIF(BTRIM(i."countymgn"), '') IS NOT NULL
                  AND UPPER(TRIM(COALESCE(i."stateprovincemgn", ''))) =
                      UPPER(TRIM(COALESCE(i."stateprovincevalidated", '')))
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "countyvalidated" = TRIM(i."countymgn")
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Validación 3a (override municipio con MGN)")

        # Validación 3b (municipio): pareja inválida → countyvalidated = NULL.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT i.ctid
                FROM "{locality}" i
                WHERE {invalid_county_pair}
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "countyvalidated" = NULL
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Validación 3b (limpiar pareja inválida)")

        # Cruce DIVIPOLA caso 1: pareja (depto validado, municipio validado) → geo_master_geography_id del municipio.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT i.ctid, m."id" AS geo_master_geography_id
                FROM "{locality}" i
                INNER JOIN "geo_master_geography" m
                  ON m."subtype" = 'municipio'
                 AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countyvalidated"))
                INNER JOIN "geo_master_geography" d
                  ON d."id" = m."parent_id"
                 AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated"))
                WHERE NULLIF(BTRIM(i."stateprovincevalidated"), '') IS NOT NULL
                  AND NULLIF(BTRIM(i."countyvalidated"), '') IS NOT NULL
                  AND i."geo_master_geography_id" IS DISTINCT FROM m."id"
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "geo_master_geography_id" = b.geo_master_geography_id
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Cruce DIVIPOLA municipio")

        # Cruce DIVIPOLA caso 2: solo depto validado → geo_master_geography_id del departamento.
        _locality_queries_helper(conn, f"""
            WITH batch AS (
                SELECT i.ctid, d."id" AS geo_master_geography_id
                FROM "{locality}" i
                INNER JOIN "geo_master_geography" d
                  ON d."subtype" = 'departamento'
                 AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated"))
                WHERE NULLIF(BTRIM(i."stateprovincevalidated"), '') IS NOT NULL
                  AND NULLIF(BTRIM(i."countyvalidated"), '') IS NULL
                  AND i."geo_master_geography_id" IS DISTINCT FROM d."id"
                LIMIT {batch_size}
            )
            UPDATE "{locality}" i
            SET "geo_master_geography_id" = b.geo_master_geography_id
            FROM batch b
            WHERE i.ctid = b.ctid
        """, label="Cruce DIVIPOLA departamento")

    logger.info("Normalización de stateprovince/county y DIVIPOLA completada en %s", locality)

# --------------------------------------------------------------------------------------------------------------------------------------
# Creación de índice BTREE sobre species para optimizar cruces taxonómicos.
# --------------------------------------------------------------------------------------------------------------------------------------

def create_species_index(db, table_name):
    # Crea índice BTREE sobre species para optimizar cruces taxonómicos.
    integrated = table_name
    with db.connect() as conn:
        idx_species = f"idx_{integrated}_species"
        logger.info("Ejecutando consulta")
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{idx_species}" ON "{integrated}" USING BTREE ("species")'
        )
        conn.commit()
        logger.info("Indice BTREE creado: %s", idx_species)
        conn.commit()


def validate_taxonomic_species(db, table_name):
    # Crea taxonomic_species_validation (borrada en tables_operations) y la puebla desde la integrada.
    # El enlace taxonomic_species_id: link_integrated_taxonomic_species_id (main).
    integrated = table_name
    species_tbl = 'taxonomic_species_validation'

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            CREATE TABLE "{species_tbl}" (
                "id"                 SERIAL PRIMARY KEY,
                "kingdom"            TEXT,
                "phylum"             TEXT,
                "class"              TEXT,
                "order"              TEXT,
                "family"             TEXT,
                "genus"              TEXT,
                "species"            TEXT NOT NULL,
                "slugspecies"        TEXT GENERATED ALWAYS AS (
                    LOWER(REPLACE(BTRIM("species"), ' ', '-'))
                ) STORED,
                "cites"              TEXT,
                "threatstatusuicn"   TEXT,
                "threatstatusmads"   TEXT,
                "exotic"             TEXT,
                "exoticriskinvasion" TEXT,
                "invasiveness"       TEXT,
                "invasive"           TEXT,
                "transplanted"       TEXT,
                "migratory"          TEXT,
                "endemic"            TEXT,
                "ismarine"           TEXT,
                "isbrackish"         TEXT,
                "isfreshwater"       TEXT,
                "isterrestrial"      TEXT,
                "referencelist"      TEXT,
                "flagtaxo"           TEXT,
                CONSTRAINT "uq_{species_tbl}_species" UNIQUE ("species")
            )
        """)
        conn.commit()
        logger.info("Tabla de validación taxonómica por especie creada: %s", species_tbl)

        logger.info("Ejecutando consulta")
        conn.execute(f"""
            INSERT INTO "{species_tbl}" (
                "kingdom", "phylum", "class", "order", "family",
                "genus", "species"
            )
            SELECT DISTINCT ON (i."species")
                i."kingdom", i."phylum", i."class", i."order", i."family",
                i."genus", i."species"
            FROM "{integrated}" i
            WHERE i."species" IS NOT NULL AND BTRIM(i."species") <> ''
            ORDER BY i."species", i."gbifid"
        """)
        conn.commit()
        logger.info(
            "Catálogo taxonómico %s poblado desde %s",
            species_tbl,
            integrated,
        )


def link_integrated_taxonomic_species_id(db, table_name):
    # Propaga id del catálogo (92k filas) a la integrada (~40M) por species.
    # CTE + LIMIT por lote: el catálogo entra en hash pequeño; la integrada se recorre por lotes.
    # Índice parcial en filas con taxonomic_species_id IS NULL evita re-escanear toda la tabla cada lote.
    # Tamaño de lote: SQL_BATCH_SIZE (FLUSH_EVERY en .env).
    integrated = table_name
    species_tbl = 'taxonomic_species_validation'
    pending_idx = f'idx_{integrated}_species_link_pending'
    batch_size = UPDATE_BATCH_SIZE
    total_linked = 0

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "taxonomic_species_id" INT4'
        )
        conn.commit()

        logger.info("Ejecutando consulta")
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS "{pending_idx}"
            ON "{integrated}" ("species")
            WHERE "taxonomic_species_id" IS NULL
              AND "species" IS NOT NULL
              AND BTRIM("species") <> ''
        """)
        conn.commit()
        logger.info("Indice parcial creado: %s", pending_idx)

        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(f"""
                WITH batch AS (
                    SELECT i.ctid, s.id AS taxonomic_species_id
                    FROM "{integrated}" i
                    INNER JOIN "{species_tbl}" s ON i."species" = s."species"
                    WHERE i."taxonomic_species_id" IS NULL
                    LIMIT {batch_size}
                )
                UPDATE "{integrated}" i
                SET "taxonomic_species_id" = b.taxonomic_species_id
                FROM batch b
                WHERE i.ctid = b.ctid
            """)
            conn.commit()
            batch_linked = result.rowcount
            conn.commit()
            if batch_linked == 0:
                break
            total_linked += batch_linked
            logger.info(
                "taxonomic_species_id batch en %s: %s filas (total %s)",
                integrated,
                f"{batch_linked:,}",
                f"{total_linked:,}",
            )

        logger.info("Ejecutando consulta")
        conn.execute(f'DROP INDEX IF EXISTS "{pending_idx}"')
        conn.commit()
        logger.info(
            "Enlace integrada → %s: %s filas con taxonomic_species_id",
            species_tbl,
            f"{total_linked:,}",
        )

    logger.info("Ejecutando vacuum analyze")
    _run_table_maintenance(db, integrated)
    logger.info("VACUUM (ANALYZE) en %s tras enlace taxonomic_species_id", integrated)


# --------------------------------------------------------------------------------------------------------------------------------------
# Tabla de localidades únicas y referencia desde la integrada
# --------------------------------------------------------------------------------------------------------------------------------------

def validate_localities(db, table_name):
    # Crea geo_locality_validation (borrada en tables_operations) y la puebla desde la integrada.
    # Completa geom por lotes. El enlace locality_id: link_integrated_locality_id (main).
    # locality_key (md5 de las 4 claves con sentinel '__NULL__') es columna generada STORED y
    # tiene UNIQUE → trata NULL como igual a NULL y habilita ON CONFLICT estricto.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    integrated = table_name
    locality_tbl = 'geo_locality_validation'
    batch_size = SQL_BATCH_SIZE
    total_updated = 0

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            CREATE TABLE "{locality_tbl}" (
                "id"                       SERIAL PRIMARY KEY,
                "decimallatitude"          DOUBLE PRECISION,
                "decimallongitude"         DOUBLE PRECISION,
                "stateprovince"            TEXT,
                "county"                   TEXT,
                "locality_key"             TEXT GENERATED ALWAYS AS (
                    md5(
                        coalesce("decimallatitude"::text, '__NULL__') || '|' ||
                        coalesce("decimallongitude"::text, '__NULL__') || '|' ||
                        coalesce("stateprovince", '__NULL__') || '|' ||
                        coalesce("county", '__NULL__')
                    )
                ) STORED,
                "geom"                     geometry(Point, 4326),
                "stateprovincemgn"         TEXT,
                "countymgn"                TEXT,
                "maritimeregion"           TEXT,
                "narinomaritimeregion"     TEXT,
                "stateprovincevalidated"   TEXT,
                "countyvalidated"          TEXT,
                "stateprovinceslug"        TEXT,
                "countyslug"               TEXT,
                "stateprovincevalidation"  BOOLEAN,
                "countyvalidation"         BOOLEAN,
                "flaggeo"                  TEXT,
                "geo_master_geography_id"  INT4,
                CONSTRAINT "uq_{locality_tbl}_locality_key" UNIQUE ("locality_key")
            )
        """)
        conn.commit()
        logger.info("Tabla de validación de localidades creada: %s", locality_tbl)

        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "locality_id" INT4'
        )
        conn.commit()
        logger.info("Columna locality_id creada en %s", integrated)
        # Inserta combinaciones nuevas desde integrated. ON CONFLICT (locality_key) cubre NULL
        # como igual a NULL (la 4-tupla queda única vía locality_key).
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            INSERT INTO "{locality_tbl}"
                ("decimallatitude", "decimallongitude", "stateprovince", "county")
            SELECT DISTINCT
                i."decimallatitude",
                i."decimallongitude",
                i."stateprovince",
                i."county"
            FROM "{integrated}" i
            ON CONFLICT ("locality_key") DO NOTHING
        """)
        conn.commit()
        logger.info("Combinaciones nuevas de integrada insertadas en %s: %s", integrated, locality_tbl)
        # Completa geom por lotes solo cuando falta geometría y hay coordenadas.
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(f"""
                WITH batch AS (
                    SELECT ctid
                    FROM "{locality_tbl}"
                    WHERE "geom" IS NULL
                      AND "decimallatitude" IS NOT NULL
                      AND "decimallongitude" IS NOT NULL
                    LIMIT {batch_size}
                )
                UPDATE "{locality_tbl}" t
                SET "geom" = ST_SetSRID(ST_MakePoint(t."decimallongitude", t."decimallatitude"), 4326)
                FROM batch
                WHERE t.ctid = batch.ctid
            """)
            conn.commit()
            batch_updated = result.rowcount
            conn.commit()
            if batch_updated == 0:
                break
            total_updated += batch_updated
            logger.info(
                "Geom batch en %s: %s filas (total %s)",
                locality_tbl,
                f"{batch_updated:,}",
                f"{total_updated:,}",
            )
        conn.commit()
        logger.info(
            "%s actualizada desde %s (geom=%s; enlace en link_integrated_locality_id)",
            locality_tbl,
            integrated,
            f"{total_updated:,}",
        )

        # Índice GIST sobre geom: requisito para que ST_Intersects en spatials_joins
        # use bound-loop con el GIST del polígono indexado en lugar de seq scan.
        conn.execute(f"SET maintenance_work_mem = '{_MAINTENANCE_WORK_MEM}'")
        conn.execute(f"SET max_parallel_maintenance_workers = {_MAX_PARALLEL_MAINTENANCE_WORKERS}")
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_{locality_tbl}_geom"
            ON "{locality_tbl}" USING GIST ("geom")
        """)
        conn.commit()
        logger.info("Índice GIST creado en %s.geom", locality_tbl)


def link_integrated_locality_id(db, table_name):
    # Propaga id de geo_locality_validation (~2M) a la integrada (~40M) por lat/lon/state/county.
    # Join por igualdad sobre locality_key (md5 con sentinel '__NULL__'); la UNIQUE de
    # geo_locality_validation ya provee el índice btree usado por el join.
    integrated = table_name
    locality_tbl = 'geo_locality_validation'
    fk_name = f"fk_{integrated}_locality_id"
    pending_gbifid_idx = f'idx_{integrated}_locality_pending_gbifid'
    batch_size = UPDATE_BATCH_SIZE  # prueba locality, en caso de error se puede cambiar a 50_000 para comenzar a probar valor
    total_linked = 0
    last_gbifid = 0

    with db.connect() as conn:
        logger.info("Ejecutando add_locality_id_column")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "locality_id" INT4'
        )
        conn.commit()

        logger.info(
            "Creando índice parcial gbifid para filas con locality_id IS NULL: %s",
            pending_gbifid_idx,
        )
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS "{pending_gbifid_idx}"
            ON "{integrated}" ("gbifid")
            WHERE "locality_id" IS NULL
        """)
        conn.commit()
        logger.info("Índice parcial creado: %s", pending_gbifid_idx)

        while True:
            logger.info(
                "locality_id: seleccionando hasta %s gbifid pendientes (después de %s)",
                f"{batch_size:,}",
                f"{last_gbifid:,}",
            )
            gbifid_rows = conn.execute(
                f"""
                SELECT "gbifid"
                FROM "{integrated}"
                WHERE "locality_id" IS NULL AND "gbifid" > %s
                ORDER BY "gbifid"
                LIMIT {batch_size}
                """,
                (last_gbifid,),
            ).fetchall()
            conn.commit()
            if not gbifid_rows:
                break
            gbifids = [row[0] for row in gbifid_rows]
            last_gbifid = gbifids[-1]

            logger.info(
                "locality_id: actualizando %s filas (gbifid hasta %s)",
                f"{len(gbifids):,}",
                f"{last_gbifid:,}",
            )
            result = conn.execute(
                f"""
                UPDATE "{integrated}" i
                SET "locality_id" = lk."id"
                FROM "{locality_tbl}" lk
                WHERE i."gbifid" = ANY(%s::bigint[])
                  AND lk."locality_key" = md5(
                      coalesce(i."decimallatitude"::text, '__NULL__') || '|' ||
                      coalesce(i."decimallongitude"::text, '__NULL__') || '|' ||
                      coalesce(i."stateprovince", '__NULL__') || '|' ||
                      coalesce(i."county", '__NULL__')
                  )
                """,
                (gbifids,),
            )
            conn.commit()
            batch_linked = result.rowcount
            total_linked += batch_linked
            logger.info(
                "locality_id batch en %s: %s filas enlazadas (total %s; avance gbifid %s)",
                integrated,
                f"{batch_linked:,}",
                f"{total_linked:,}",
                f"{last_gbifid:,}",
            )
            if len(gbifids) < batch_size:
                break

        logger.info("Ejecutando consulta")
        conn.execute(f'DROP INDEX IF EXISTS "{pending_gbifid_idx}"')
        conn.commit()
        logger.info("Índice parcial eliminado: %s", pending_gbifid_idx)

        logger.info("Ejecutando consulta")
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{integrated}_locality_id" '
            f'ON "{integrated}" USING BTREE ("locality_id")'
        )
        conn.commit()
        logger.info("Indice creado en %s: locality_id", integrated)

        logger.info("Ejecutando consulta")
        conn.execute(f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{fk_name}"')
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            ALTER TABLE "{integrated}"
            ADD CONSTRAINT "{fk_name}"
            FOREIGN KEY ("locality_id")
            REFERENCES "geo_locality_validation" ("id")
            ON UPDATE CASCADE
            ON DELETE SET NULL
            NOT VALID
        """)
        conn.commit()
        logger.info(
            "Enlace integrada → geo_locality_validation: %s filas con locality_id",
            f"{total_linked:,}",
        )

    logger.info("Ejecutando vacuum analyze")
    _run_table_maintenance(db, integrated)
    logger.info("VACUUM (ANALYZE) en %s tras locality_id y FK", integrated)


# --------------------------------------------------------------------------------------------------------------------------------------
# Validaciones geográficas
# --------------------------------------------------------------------------------------------------------------------------------------

def validate_geography(db, table_name):
    # Valida geografía en geo_locality_validation
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    _ = table_name
    locality_tbl = 'geo_locality_validation'
    batch_size = UPDATE_BATCH_SIZE
    with db.connect() as conn:
        # 1/3 stateprovincevalidation
        last_id = 0
        total_sp = 0
        while True:
            # Se actualiza stateprovincevalidation por lotes (ctid + LIMIT) para acotar WAL y locks.
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f"""
                WITH batch AS (
                    SELECT t.ctid
                    FROM "{locality_tbl}" t
                    WHERE t."id" > %s
                    ORDER BY t."id"
                    LIMIT {batch_size}
                )
                UPDATE "{locality_tbl}" t SET
                    "stateprovincevalidation" = CASE
                        WHEN NULLIF(BTRIM(t."stateprovincevalidated"), '') IS NULL THEN NULL
                        WHEN UPPER(TRIM(t."stateprovincevalidated"))
                             = UPPER(TRIM(COALESCE(t."stateprovincemgn", ''))) THEN TRUE
                        WHEN (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL)
                             OR (COALESCE(t."decimallatitude", 0) = 0
                                 AND COALESCE(t."decimallongitude", 0) = 0)
                            THEN NULL
                        WHEN NULLIF(BTRIM(t."maritimeregion"), '') IS NOT NULL THEN NULL
                        ELSE FALSE
                    END
                FROM batch b
                WHERE t.ctid = b.ctid
                RETURNING t."id"
                """,
                (last_id,),
            )
            conn.commit()
            batch_updated = result.rowcount
            id_rows = result.fetchall()
            if batch_updated == 0:
                break
            last_id = max(r[0] for r in id_rows)
            conn.commit()
            total_sp += batch_updated
            logger.info(
                "Validación geográfica (1/3 stateprovince) batch en %s: %s filas (total %s, hasta id=%s)",
                locality_tbl,
                f"{batch_updated:,}",
                f"{total_sp:,}",
                last_id,
            )
        logger.info(
            "Validación geográfica (1/3 stateprovince) completada en %s (%s filas)",
            locality_tbl,
            f"{total_sp:,}",
        )

    # --- 2/3 countyvalidation ---
    with db.connect() as conn:
        last_id = 0
        total_co = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f"""
                WITH batch AS (
                    SELECT t.ctid
                    FROM "{locality_tbl}" t
                    WHERE t."id" > %s
                    ORDER BY t."id"
                    LIMIT {batch_size}
                )
                UPDATE "{locality_tbl}" t SET
                    "countyvalidation" = CASE
                        WHEN NULLIF(BTRIM(t."countyvalidated"), '') IS NULL THEN NULL
                        WHEN UPPER(TRIM(t."countyvalidated"))
                             = UPPER(TRIM(COALESCE(t."countymgn", ''))) THEN TRUE
                        WHEN (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL)
                             OR (COALESCE(t."decimallatitude", 0) = 0
                                 AND COALESCE(t."decimallongitude", 0) = 0)
                            THEN NULL
                        WHEN NULLIF(BTRIM(t."maritimeregion"), '') IS NOT NULL THEN NULL
                        ELSE FALSE
                    END
                FROM batch b
                WHERE t.ctid = b.ctid
                RETURNING t."id"
                """,
                (last_id,),
            )
            conn.commit()
            batch_updated = result.rowcount
            id_rows = result.fetchall()
            if batch_updated == 0:
                break
            last_id = max(r[0] for r in id_rows)
            conn.commit()
            total_co += batch_updated
            logger.info(
                "Validación geográfica (2/3 county) batch en %s: %s filas (total %s, hasta id=%s)",
                locality_tbl,
                f"{batch_updated:,}",
                f"{total_co:,}",
                last_id,
            )
        logger.info(
            "Validación geográfica (2/3 county) completada en %s (%s filas)",
            locality_tbl,
            f"{total_co:,}",
        )

    # --- 3/3 flaggeo ---
    with db.connect() as conn:
        last_id = 0
        total_fg = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f"""
                WITH batch AS (
                    SELECT t.ctid
                    FROM "{locality_tbl}" t
                    WHERE t."id" > %s
                    ORDER BY t."id"
                    LIMIT {batch_size}
                )
                UPDATE "{locality_tbl}" t SET
                    "flaggeo" = CASE
                        WHEN t."stateprovincevalidation" IS FALSE
                             AND t."countyvalidation" IS FALSE
                            THEN 'Departamento y municipio no coinciden con ubicación de la coordenada'
                        WHEN t."stateprovincevalidation" IS TRUE
                             AND t."countyvalidation" IS FALSE
                            THEN 'Municipio no coincide con ubicación de la coordenada'
                        WHEN t."stateprovincevalidation" IS FALSE
                             AND t."countyvalidation" IS TRUE
                            THEN 'Departamento no coincide con ubicación de la coordenada'
                        WHEN t."stateprovincevalidation" IS NULL
                             AND t."countyvalidation" IS NULL
                             AND NULLIF(BTRIM(t."maritimeregion"), '') IS NOT NULL
                            THEN 'Coordenada en área marítima'
                        WHEN t."stateprovincevalidation" IS NULL
                             AND t."countyvalidation" IS NULL
                             AND (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL
                                  OR (COALESCE(t."decimallatitude", 0) = 0
                                      AND COALESCE(t."decimallongitude", 0) = 0))
                            THEN 'Sin coordenadas'
                        ELSE NULL
                    END
                FROM batch b
                WHERE t.ctid = b.ctid
                RETURNING t."id"
                """,
                (last_id,),
            )
            conn.commit()
            batch_updated = result.rowcount
            id_rows = result.fetchall()
            if batch_updated == 0:
                break
            last_id = max(r[0] for r in id_rows)
            conn.commit()
            total_fg += batch_updated
            logger.info(
                "Validación geográfica (3/3 flaggeo) batch en %s: %s filas (total %s, hasta id=%s)",
                locality_tbl,
                f"{batch_updated:,}",
                f"{total_fg:,}",
                last_id,
            )
        logger.info(
            "Validación geográfica (3/3 flaggeo) completada en %s (%s filas)",
            locality_tbl,
            f"{total_fg:,}",
        )


    _run_table_maintenance(db, locality_tbl)
    logger.info("VACUUM (ANALYZE) en %s tras validación geográfica (tabla de localidades)", locality_tbl)


def populate_geo_slugs(db):
    """Persiste slugs departamentales y municipales desde geo_master_geography."""
    locality_tbl = 'geo_locality_validation'
    batch_size = UPDATE_BATCH_SIZE
    total_updated = 0
    last_id = 0

    with db.connect() as conn:
        while True:
            result = conn.execute(
                f"""
                WITH batch AS (
                    SELECT gl.id, gl.geo_master_geography_id
                    FROM "{locality_tbl}" gl
                    WHERE gl.id > %s
                      AND gl.geo_master_geography_id IS NOT NULL
                    ORDER BY gl.id
                    LIMIT %s
                )
                UPDATE "{locality_tbl}" gl SET
                    stateprovinceslug = d.slug,
                    countyslug = m.slug
                FROM batch b
                JOIN geo_master_geography gm ON gm.id = b.geo_master_geography_id
                LEFT JOIN geo_master_geography m
                    ON m.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
                LEFT JOIN geo_master_geography d
                    ON d.id = COALESCE(
                        m.parent_id,
                        CASE WHEN gm.subtype = 'departamento' THEN gm.id END
                    )
                WHERE gl.id = b.id
                RETURNING gl.id
                """,
                (last_id, batch_size),
            )
            conn.commit()
            batch_updated = result.rowcount
            id_rows = result.fetchall()
            if batch_updated == 0:
                break
            last_id = max(r[0] for r in id_rows)
            total_updated += batch_updated
            logger.info(
                'Slugs geo batch en %s: %s filas (total %s, hasta id=%s)',
                locality_tbl,
                f'{batch_updated:,}',
                f'{total_updated:,}',
                last_id,
            )

    logger.info('Slugs geo completados en %s (%s filas)', locality_tbl, f'{total_updated:,}')
    _run_table_maintenance(db, locality_tbl)

# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces taxonómicos con listados de referencia
# --------------------------------------------------------------------------------------------------------------------------------------

# Se definen las tablas y los campos a cruzar. La idea es iterar sobre las tablas y campos para evitar
# tener que definirlas las consultas SQL manualmente.
# Los cruces actualizan taxonomic_species_validation (v) por species; la integrada enlaza por taxonomic_species_id.
# Es equivalente a ejecutar la siguiente consulta:
# UPDATE "taxonomic_species_validation" v SET "cites" = t."cites" FROM "taxonomic_cites" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "threatstatusuicn" = t."threatstatus" FROM "taxonomic_threat_iucn" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "threatstatusmads" = t."threatstatus" FROM "taxonomic_threat_mads" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "exotic" = t."exotic", ... FROM "taxonomic_invasive_exotic" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "migratory" = t."migratory", "endemic" = t."endemic" FROM "taxonomic_col_list" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "referencelist" = t."datasetid" FROM "taxonomic_col_list" t WHERE v."species" = t."species"
# UPDATE ... SET ismarine/isbrackish/isfreshwater/isterrestrial desde taxonomic_worms.environmentaphiaworms (ILIKE)
_FLAGTAXO_CLASSES = ('Aves', 'Mammalia', 'Reptilia', 'Squamata', 'Crocodylia', 'Testudines')
_FLAGTAXO_ORDERS = ('Lepidoptera','Odonota')

_TAXONOMIC_JOINS = {
    'taxonomic_cites': {
        'columns': {'cites': 'cites'},
    },
    'taxonomic_threat_iucn': {
        'columns': {'threatstatus': 'threatstatusuicn'},
    },
    'taxonomic_threat_mads': {
        'columns': {'threatstatus': 'threatstatusmads'},
    },
    'taxonomic_invasive_exotic': {
        'columns': {
            'exotic': 'exotic',
            'exoticriskinvasion': 'exoticriskinvasion',
            'invasiveness': 'invasiveness',
            'invasive': 'invasive',
            'transplanted': 'transplanted',
        },
    },
    'taxonomic_migratory': {
        'columns': {
            'migratory': 'migratory',
        },
    },
    'taxonomic_col_list': {
        'columns': {
            'migratory': 'migratory',
            'endemic': 'endemic',
            'datasetid': 'referencelist',
        },
    },
    'taxonomic_worms': {
        'source_column': 'environmentaphiaworms',
        'columns': {
            'ismarine': 'Marine',
            'isbrackish': 'Brackish',
            'isfreshwater': 'Freshwater',
            'isterrestrial': 'Terrestrial',
        },
    },
}


def _taxonomic_join_set_parts(col_map, *, source_column=None):
    """Arma queries SET para UPDATE ... FROM en taxonomic_joins."""
    set_parts = []
    if source_column:
        for dest_col, flag in col_map.items():
            set_parts.append(
                f'"{dest_col}" = CASE '
                f'WHEN t."{source_column}" ILIKE \'%{flag}%\' THEN \'{flag}\' '
                f'ELSE v."{dest_col}" END'
            )
        return set_parts
    for src, dest in col_map.items():
        if dest == 'migratory':
            set_parts.append(
                f'"{dest}" = CASE '
                f'WHEN v."migratory" IS NULL THEN t."{src}" '
                f'ELSE v."migratory" END'
            )
        else:
            set_parts.append(f'"{dest}" = t."{src}"')
    return set_parts


def taxonomic_joins(db, table_name):
    # Cruza taxonomic_species_validation con tablas taxonómicas por el campo species.
    # table_name se conserva por compatibilidad con el orquestador.
    species_tbl = 'taxonomic_species_validation'
    with db.connect() as conn:
        for src_table, config in _TAXONOMIC_JOINS.items():
            col_map = config['columns']
            set_parts = _taxonomic_join_set_parts(
                col_map,
                source_column=config.get('source_column'),
            )
            set_clause = ',\n                    '.join(set_parts)
            logger.info("Ejecutando consulta")
            conn.execute(f"""
                UPDATE "{species_tbl}" v
                SET {set_clause}
                FROM "{src_table}" t
                WHERE v."species" = t."species"
            """)
            conn.commit()
            logger.info("Join con %s completado en %s", src_table, species_tbl)

        logger.info("Ejecutando consulta")
        conn.execute(f"""
            UPDATE "{species_tbl}"
            SET "referencelist" = 'Presente en lista taxonómica: ' || "referencelist"
            WHERE "referencelist" IS NOT NULL
        """)
        conn.commit()
        logger.info("Campo referencelist actualizado en %s", species_tbl)

        classes_list = ', '.join(f"'{c}'" for c in _FLAGTAXO_CLASSES)
        orders_list = ', '.join(f"'{o}'" for o in _FLAGTAXO_ORDERS)

        logger.info("Ejecutando consulta")
        conn.execute(f"""
            UPDATE "{species_tbl}"
            SET "flagtaxo" = CASE
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "transplanted" = 'Trasplantada'
                    THEN 'Ausente en lista taxonómica_Trasplantada'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "migratory" = 'Migratorio'
                    THEN 'Ausente en lista taxonómica_Migratoria'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "exoticriskinvasion" = 'Exótica con potencial de invasión'
                    THEN 'Ausente en lista taxonómica_Exótica con potencial de invasión'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "invasive" = 'Invasora'
                    THEN 'Ausente en lista taxonómica_Invasora'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "exotic" = 'Exótica'
                    THEN 'Ausente en lista taxonómica_Exótica'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "class" IN ({classes_list})
                    THEN 'Ausente en lista taxonómica'
                WHEN "referencelist" IS NULL AND "species" IS NOT NULL
                     AND "order" IN ({orders_list})
                    THEN 'Ausente en lista taxonómica'
                ELSE NULL
            END
        """)
        conn.commit()
        logger.info("Campo flagtaxo completado en %s", species_tbl)

        conn.commit()

# --------------------------------------------------------------------------------------------------------------------------------------
# Normalización de campos threatstatus
# --------------------------------------------------------------------------------------------------------------------------------------

def clean_threatstatus_fields(db, table_name):
    # Normaliza threatstatus y agrega sufijos por fuente (IUCN/MADS) en taxonomic_species_validation.
    # table_name se conserva por compatibilidad con el orquestador; el trabajo es sobre la tabla de especies.
    species_tbl = 'taxonomic_species_validation'
    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            UPDATE "{species_tbl}"
            SET "threatstatusuicn" = NULLIF(TRIM("threatstatusuicn"), ''),
                "threatstatusmads" = NULLIF(TRIM("threatstatusmads"), '')
            WHERE "threatstatusuicn" IS NOT NULL OR "threatstatusmads" IS NOT NULL
        """)
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(f"""
            UPDATE "{species_tbl}"
            SET "threatstatusuicn" = CASE
                    WHEN "threatstatusuicn" IS NULL THEN NULL
                    WHEN "threatstatusuicn" LIKE '%_IUCN' THEN "threatstatusuicn"
                    ELSE "threatstatusuicn" || '_IUCN'
                END,
                "threatstatusmads" = CASE
                    WHEN "threatstatusmads" IS NULL THEN NULL
                    WHEN "threatstatusmads" LIKE '%_MADS' THEN "threatstatusmads"
                    ELSE "threatstatusmads" || '_MADS'
                END
        """)
        conn.commit()
        logger.info(
            "Validación de threatstatus (vacíos/sufijos por fuente) completada en %s (integrada: %s)",
            species_tbl,
            table_name,
        )
        conn.commit()

    logger.info("Ejecutando vacuum analyze")
    _run_table_maintenance(db, species_tbl)
    logger.info("VACUUM (ANALYZE) en %s tras cruces y normalización threatstatus", species_tbl)

# --------------------------------------------------------------------------------------------------------------------------------------
# Backfill desde API GBIF
# --------------------------------------------------------------------------------------------------------------------------------------

def _fetch_gbif_json(url, key, label, retries=5, backoff_factor=0.5):
    retry_statuses = {429, 500, 502, 503, 504}
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                return json.loads(response.read().decode('utf-8')), True
        except urllib.error.HTTPError as e:
            if e.code in retry_statuses and attempt < retries:
                time.sleep(backoff_factor * (2 ** attempt))
                continue
            logger.warning("GBIF API status %s para %s %s", e.code, label, key)
            return None, False
        except urllib.error.URLError as e:
            if attempt < retries:
                time.sleep(backoff_factor * (2 ** attempt))
                continue
            logger.warning("Error de red consultando GBIF API para %s %s: %s", label, key, e.reason)
            return None, False
        except Exception as e:
            logger.warning("Error consultando GBIF API para %s %s: %s", label, key, e)
            return None, False
    return None, False


def _parse_gbif_created_date(value):
    # Convierte el campo created de GBIF a date (YYYY-MM-DD).
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace('Z', '+00:00')).date()
    except ValueError:
        try:
            return datetime.strptime(raw[:10], '%Y-%m-%d').date()
        except ValueError:
            logger.warning("Fecha created inválida en respuesta GBIF: %s", value)
            return None


_DATASET_UPSERT_SQL = """
    INSERT INTO gbif_datasets (datasetkey, license, doi, datasettitle, logourl, datatype, created)
    VALUES (%(datasetkey)s, %(license)s, %(doi)s, %(datasettitle)s, %(logourl)s, %(datatype)s, %(created)s)
    ON CONFLICT (datasetkey) DO UPDATE
    SET license      = EXCLUDED.license,
        doi          = EXCLUDED.doi,
        datasettitle = EXCLUDED.datasettitle,
        logourl      = EXCLUDED.logourl,
        datatype     = EXCLUDED.datatype,
        created      = EXCLUDED.created
"""

_PUBLISHER_UPSERT_SQL = """
    INSERT INTO gbif_publishers (publishingorgkey, organization, institutionid)
    VALUES (%(publishingorgkey)s, %(organization)s, %(institutionid)s)
    ON CONFLICT (publishingorgkey) DO UPDATE
    SET organization  = EXCLUDED.organization,
        institutionid = EXCLUDED.institutionid
"""


def _enrich_from_gbif_api(conn, integrated, *,
                          integrated_column,
                          catalog_table,
                          catalog_title_col,
                          api_url_template,
                          upsert_sql,
                          upsert_kwargs,
                          log_label):
    """Backfill genérico de catálogo GBIF desde la API.

    Lee de `integrated` las claves faltantes (o sin título/organización) en el catálogo,
    consulta la API GBIF en paralelo (ThreadPoolExecutor) y hace UPSERT con ON CONFLICT.
    Retorna (n_faltantes, n_consultados, n_upsertados, n_errores).
    """
    rows = conn.execute(f"""
        SELECT DISTINCT i."{integrated_column}"
        FROM "{integrated}" i
        LEFT JOIN "{catalog_table}" c
          ON i."{integrated_column}" = c."{integrated_column}"
        WHERE i."{integrated_column}" IS NOT NULL
          AND (c."{integrated_column}" IS NULL OR c."{catalog_title_col}" IS NULL)
    """).fetchall()
    conn.commit()
    missing = [r[0] for r in rows if r[0]]
    logger.info("%s: %s claves faltantes", log_label, f"{len(missing):,}")

    fetched = upserted = errors = 0
    if missing:
        max_workers = min(20, max(4, len(missing)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_gbif_json,
                                api_url_template.format(key=k),
                                k,
                                integrated_column): k
                for k in missing
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    data, ok = future.result()
                except Exception as e:
                    logger.warning("Error tarea GBIF %s %s: %s", integrated_column, key, e)
                    errors += 1
                    continue
                if not ok:
                    errors += 1
                    continue
                fetched += 1
                conn.execute(upsert_sql, upsert_kwargs(data, key))
                conn.commit()
                upserted += 1
    return len(missing), fetched, upserted, errors


def gbif_api_calls(db, table_name):
    """Backfill de gbif_datasets y gbif_publishers desde la API GBIF; añade FK NOT VALID
    hacia ambos catálogos. Filas con clave NULL no participan en la FK."""
    integrated = table_name
    with db.connect() as conn:
        ds_total, ds_fetched, ds_upserted, ds_errors = _enrich_from_gbif_api(
            conn, integrated,
            integrated_column='datasetkey',
            catalog_table='gbif_datasets',
            catalog_title_col='datasettitle',
            api_url_template='https://api.gbif.org/v1/dataset/{key}',
            upsert_sql=_DATASET_UPSERT_SQL,
            upsert_kwargs=lambda data, key: {
                'datasetkey': data.get('key') or key,
                'license': data.get('license'),
                'doi': data.get('doi'),
                'datasettitle': data.get('title'),
                'logourl': data.get('logoUrl'),
                'datatype': data.get('type'),
                'created': _parse_gbif_created_date(data.get('created')),
            },
            log_label='GBIF datasets',
        )
        pub_total, pub_fetched, pub_upserted, pub_errors = _enrich_from_gbif_api(
            conn, integrated,
            integrated_column='publishingorgkey',
            catalog_table='gbif_publishers',
            catalog_title_col='organization',
            api_url_template='https://api.gbif.org/v1/organization/{key}',
            upsert_sql=_PUBLISHER_UPSERT_SQL,
            upsert_kwargs=lambda data, key: {
                'publishingorgkey': data.get('key') or key,
                'organization': data.get('title'),
                'institutionid': f"https://www.gbif.org/publisher/{data.get('key') or key}",
            },
            log_label='GBIF publishers',
        )

        # Integridad referencial: FK NOT VALID (no escanea huérfanos). VALIDATE CONSTRAINT
        # se ejecuta aparte cuando no queden huérfanos.
        for fk_name, fk_column, ref_table in (
            (f"fk_{integrated}_gbif_datasetkey",       'datasetkey',       'gbif_datasets'),
            (f"fk_{integrated}_gbif_publishingorgkey", 'publishingorgkey', 'gbif_publishers'),
        ):
            conn.execute(f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{fk_name}"')
            conn.execute(f"""
                ALTER TABLE "{integrated}"
                ADD CONSTRAINT "{fk_name}"
                FOREIGN KEY ("{fk_column}")
                REFERENCES "{ref_table}" ("{fk_column}")
                ON UPDATE CASCADE
                ON DELETE NO ACTION
                NOT VALID
            """)
            conn.commit()
            logger.info("FK %s añadida en %s (NOT VALID)", fk_name, integrated)

    logger.info(
        "GBIF datasets en %s: faltantes=%s consultados=%s upserts=%s errores=%s",
        integrated, f"{ds_total:,}", f"{ds_fetched:,}", f"{ds_upserted:,}", f"{ds_errors:,}",
    )
    logger.info(
        "GBIF publishers en %s: faltantes=%s consultados=%s upserts=%s errores=%s",
        integrated, f"{pub_total:,}", f"{pub_fetched:,}", f"{pub_upserted:,}", f"{pub_errors:,}",
    )
