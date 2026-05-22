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
- validate_taxonomic_species: Tabla taxonomic_species_validation por species únicos y enlace taxonomic_species_id en integrada.
- link_integrated_taxonomic_species_id: UPDATE por lotes (CTE) integrada → catálogo por species; índice parcial y VACUUM ANALYZE en integrada.
- link_integrated_locality_id: UPDATE por lotes (CTE) integrada → geo_locality_validation por 4 campos; índice parcial, FK NOT VALID y VACUUM.
- spatials_joins: Cruza geo_locality_validation con MGN_ADM_MPIO_2025 y capas marítimas usando ST_Intersects.
- normalize_stateprovince_county: Normaliza stateprovince, county y slugs en geo_locality_validation antes de validar geografía.
- validate_geography: Valida geografía en geo_locality_validation (tres bloques con db.connect: depto, municipio, flaggeo).
- taxonomic_joins: Cruza taxonomic_species_validation con tablas taxonómicas por species.
- clean_threatstatus_fields: Normaliza threatstatus en taxonomic_species_validation (IUCN/MADS).
- gbif_api_calls: Completa gbif_datasets y gbif_publishers desde tablas locales y API GBIF; añade FK NOT VALID desde la integrada hacia esas tablas (validar aparte con VALIDATE CONSTRAINT).
"""

import csv
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import urllib.error
import urllib.request

from utils.connection import table_exists

# Inicialización del logger
logger = logging.getLogger('sintesis_biocifras')

# Tamaño de buffer para COPY (FLUSH_EVERY en .env; valores altos reducen round-trips al servidor).
SQL_BATCH_SIZE = int(os.getenv('FLUSH_EVERY', '1000000'))
# Límite de filas por sentencia en UPDATE por lotes (JOIN + ctid). Valores del orden de 1e6 hacen que cada
# iteración tarde mucho sin commit intermedio; en pg_stat_activity parece “quieto” hasta terminar el lote.
UPDATE_BATCH_SIZE = int(os.getenv('UPDATE_BATCH_SIZE', '50000'))


# ------------------------------------------------------------------------------------------------------------
# Definición de listas y variables para el proceso de carga desde los archivos TSV de GBIF
# 
# Para el process de carga desde los archivos integrated.csv, ocurrence.txt y sql.csv se definen únicamente 
# las columnas con listas que se van a utilizar para evitar cargar datos innecesarios y optimizar el 
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
    # Se ejecuta el comando VACUUM (ANALYZE) y/o VACUUM (FULL, ANALYZE) para mantener la tabla optimizada.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla a mantener.
    # Retorna:
    # - None: No retorna nada.
    raw_conn = db.raw_connection()
    try:
        raw_conn.autocommit = True
        with raw_conn.cursor() as cur:
            cur.execute(f'VACUUM (ANALYZE) "{table_name}"')
            # Si la variable de entorno RUN_VACUUM_FULL es true, se ejecuta el comando VACUUM (FULL, ANALYZE).
            if os.getenv('RUN_VACUUM_FULL', 'false').lower() == 'true':
                cur.execute(f'VACUUM (FULL, ANALYZE) "{table_name}"')
    finally:
        raw_conn.close()

# -------------------------------------------------------------------------------------------------------------------------
# Creacion / truncado de tablas de staging (integrates y ocurrence) y la tabla integrada (dwc_integrated)
# -------------------------------------------------------------------------------------------------------------------------

def _build_create_ddl(table_name, col_types):
    # Función de apoyo.
    # Genera sentencias CREATE TABLE a partir del diccionario columna -> tipo SQL.
    # cols es un diccionario con el nombre de la columna y el tipo SQL que se genera dinámicamente
    # col_types es uno de los diccionarios: _SIMPLE_TYPES, _OCURRENCE_TYPES, _SQL_COL_TYPES
    # Es equivalente a ejecutar la siguiente consulta:
    # CREATE UNLOGGED TABLE "tabla_fecha" (...) WITH (autovacuum_enabled = false);
    # Parámetros:
    # - table_name: Nombre de la tabla a crear.
    # - col_types: Diccionario con los tipos de columnas para la tabla: _SIMPLE_TYPES, _OCURRENCE_TYPES, _SQL_COL_TYPES
    # Retorna:
    # - ddl: Sentencia CREATE TABLE para la tabla.
    cols = ', '.join(f'"{col}" {dtype}' for col, dtype in col_types.items())
    return (
        f'CREATE UNLOGGED TABLE "{table_name}" ({cols}) '
        f'WITH (autovacuum_enabled = false);'
    )

# Para mantener un historial de las tablas de staging y la tabla integrada se utiliza un sufijo de fecha.
def tables_operations(db, suffix, upload_type=None):
    # Crea tablas con sufijo de fecha. Si ya existen, las elimina y vuelven a crear para garantizar una carga limpia.
    # Recrea staging, elimina la integrada con sufijo y luego las tablas de validación (sin recrearlas aquí).
    # Se tienen el cuenta el tipo de carga: sql o regular.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - suffix: Sufijo de fecha para las tablas de staging y la tabla integrada.
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
        table_names = {'sql': f'dwc_sql_{suffix}'}
        type_maps = {'sql': _SQL_COL_TYPES}
        keys = ('sql',)
    else:
        table_names = {
            'occurrence': f'dwc_occurrence_{suffix}',
            'verbatim': f'dwc_verbatim_{suffix}',
            'integrated': f'dwc_integrated_{suffix}',
        }
        type_maps = {
            'occurrence': _SIMPLE_TYPES,
            'verbatim': _OCURRENCE_TYPES,
        }
        keys = ('occurrence', 'verbatim')
    validation_tables = ('taxonomic_species_validation', 'geo_locality_validation')
    if True:
        with db.connect() as conn:
            for key in keys:
                tname = table_names[key]
                if table_exists(db, tname):
                    logger.info("Ejecutando consulta")
                    conn.execute(f'DROP TABLE "{tname}"')
                    conn.commit()
                    logger.info("DROP TABLE %s", tname)
                ddl = _build_create_ddl(tname, type_maps[key])
                logger.info("Ejecutando consulta")
                conn.execute(ddl)
                conn.commit()
                logger.info("CREATE TABLE %s", tname)
            integrated_tname = f'dwc_integrated_{suffix}'
            if table_exists(db, integrated_tname):
                logger.info("Ejecutando consulta")
                conn.execute(f'DROP TABLE "{integrated_tname}"')
                conn.commit()
                logger.info("DROP TABLE %s", integrated_tname)
            for vtbl in validation_tables:
                logger.info("Ejecutando consulta")
                conn.execute(f'DROP TABLE IF EXISTS "{vtbl}"')
                conn.commit()
                logger.info("DROP TABLE %s", vtbl)
            conn.commit()
    return table_names

# -------------------------------------------------------------------------------------------------------------------------
# Actualización tabla de registro
# -------------------------------------------------------------------------------------------------------------------------

def register_load(db, table_names, created_at, origin):
    # Actualiza el campo is_latest de las tablas de staging y la tabla integrada.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_names: Diccionario con los nombres de las tablas de staging y la tabla integrada.
    # - created_at: Fecha de creación de la tabla.
    # - origin: Origen de la carga: SQL o DwC-A.
    # Retorna:
    # - None: No retorna nada.
    prefixes = {
        'occurrence': 'dwc_occurrence_%',
        'verbatim': 'dwc_verbatim_%',
        'integrated': 'dwc_integrated_%',
    }
    with db.connect() as conn:
        for key, table_name in table_names.items():
            prefix = prefixes[key]
            logger.info("Ejecutando consulta")
            conn.execute(
                "UPDATE table_registry SET is_latest = FALSE "
                "WHERE table_name LIKE %(prefix)s AND is_latest = TRUE"
            , {'prefix': prefix})
            conn.commit()
            logger.info("Ejecutando consulta")
            conn.execute(
                "INSERT INTO table_registry (table_name, origin, created_at, is_latest) "
                "VALUES (%(table_name)s, %(origin)s, %(created_at)s, TRUE)"
            , {'table_name': table_name, 'origin': origin, 'created_at': created_at})
        conn.commit()
    logger.info("Datos cargados en table_registry.")


# ------------------------------------------------------------------------------------------------------------
# Carga masiva de datos desde los archivos TSV de GBIF a las tablas de staging
# ------------------------------------------------------------------------------------------------------------

# Los datos de GBIF pueden presentar problemas por el uso de caracteres especiales como comillas, tabuladores y
# backlash. Por lo que antes de subir cada batch datos se deben procesar los caracteres especiales para evitar
# errores de carga a traves de csv.writer para manejar el caracter comilla doble ("). Para backslash se indica
# en el la sentencia COPY de PostgreSQL con el delimitador E'\\'.

# El otro punto importante es que al cargar los datos se utiliza copy_expert de psycopg2 ya que es más eficiente
# al poder hacer cargas por batch y no tener que leer todo el archivo en memoria.
# Ahora, por qué se utiliza copy_expert y no copy_from o directamente con execute o una 
# consulta SQL en raw o con el comando COPY de PostgreSQL?
# La principal son los caracteres especiales desde los archivos de GBIF, además de tener control sobre la cantidad
# de filas a cargar por batch.
# COPY si bien es más rápido, hay que procesar los caracteres especiales antes de la carga, pero sobre todo los
# archivos deben estár en el mismo servidor de la base de datos, aunque puede solventarse con salida STDOUT.
# EXECUTE es más flexible, pero espera siempre que se retorne el resultado de la consulta, por lo que
# en procesos de carga masiva no es la mejor opción.
# Por último, el comando de copy_expert de psycopg2 se ejecuta a través de la conexión raw de psycopg2,
# que crea un cursor y se ejecuta el comando de copy_expert con el buffer de datos procesado por csv.writer.

# También se debe manejar fechas desde los archivos TSV de GBIF que se deben convertir a ISO 8601 para columnas TIMESTAMPTZ
# Bug que apareció al cargar los datos descargados en formato GBIF SQL
# El EPOCH es el número de milisegundos desde el 1 de enero de 1970 00:00:00 UTC, pero no es legible como el timestamp
# por lo que se debe convertir a ISO 8601 para que sea legible y que se pueda cargar a la base de datos.
_EPOCH_MS_COLS = {'lastinterpreted', 'lastparsed'}


def _epoch_ms_to_iso(value):
    # Convierte epoch en milisegundos a ISO 8601 para columnas TIMESTAMPTZ.
    # Parámetros:
    # - value: Valor a convertir.
    # Retorna:
    # - value: Valor convertido a ISO 8601.
    if not value:
        return value
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return value


def data_upload(db, filepath, table_name, columns):
    # Confirma que los archivos de datos definidos en el .env existen.
    # Si no existen, se retorna un error y se elimina la tabla de staging.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - filepath: Ruta del archivo a cargar.
    # - table_name: Nombre de la tabla a cargar.
    # - columns: Columnas a cargar.
    # Retorna:
    # - None: No retorna nada.
    if not filepath or not Path(filepath).is_file():
        with db.connect() as conn:
            logger.info("Ejecutando consulta")
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()
        logger.info("DROP TABLE %s", table_name)
        msg = (
            f"No se definió la ruta del archivo en el .env para la tabla {table_name}"
            if not filepath
            else f"El archivo no existe en la ruta indicada en el .env: {filepath}"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    # Se generan las columnas de la tabla de staging en minúsculas para la ejecución del comando COPY de PostgreSQL.
    # Primero se generan las columnas en minúsculas y luego se generan las columnas entre comillas dobles para la ejecución 
    # del comando COPY de PostgreSQL.
    # Se ejecuta el comando COPY de PostgreSQL con el formato csv, el delimitador E'\\t' y el null '' para evitar errores de carga.
    db_cols = [c.lower() for c in columns]
    quoted_cols = ', '.join(f'"{c}"' for c in db_cols)
    copy_sql = (
        f'COPY "{table_name}" ({quoted_cols}) '
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"
    )

    # Se crea una conexión raw para ejecutar el comando COPY de PostgreSQL usando psycopg2.
    # El tamaño de lote COPY es SQL_BATCH_SIZE (variable FLUSH_EVERY en .env al importar este módulo).
    raw_conn = db.raw_connection()
    cur = None
    flush_size = SQL_BATCH_SIZE
    try:
        cur = raw_conn.cursor()
        cur.execute("SET synchronous_commit = OFF")
        # Parametros de sesion orientados a cargas masivas por COPY.
        # maintenance_work_mem aporta sobre todo en CREATE INDEX, pero se deja configurable en .env.
        cur.execute(f"SET maintenance_work_mem = '{os.getenv('MAINTENANCE_WORK_MEM', '2GB')}'")
        cur.execute(f"SET work_mem = '{os.getenv('WORK_MEM', '64MB')}'")
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        count = 0
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            # Se arma una sola vez el mapeo columna -> posición para evitar overhead de DictReader por fila.
            header = next(reader, None)
            if not header:
                raise ValueError(f"Archivo vacío o sin encabezado: {filepath}")
            header_map = {name.lower(): idx for idx, name in enumerate(header)}
            col_specs = []
            for c in columns:
                name = c.lower()
                col_specs.append((header_map.get(name), name in _EPOCH_MS_COLS))

            for row in reader:
                # Para cada fila, escribe solo las columnas requeridas. Si falta columna o valor, usa cadena vacía.
                # Si la columna es de tipo TIMESTAMPTZ, se convierte a ISO 8601.
                writer.writerow([
                    _epoch_ms_to_iso(row[idx]) if is_epoch and idx is not None and idx < len(row)
                    else (row[idx] if idx is not None and idx < len(row) else '')
                    for idx, is_epoch in col_specs
                ])
                count += 1
                # Si el modulo de count con flush_size es igual a 0, se envía el buffer a la base de datos.
                if count % flush_size == 0:
                    buffer.seek(0)
                    cur.copy_expert(copy_sql, buffer)
                    raw_conn.commit()
                    # Se reinicia el buffer y el writer para la siguiente carga.
                    buffer = io.StringIO()
                    writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                    logger.info("  %s — %s filas cargadas...", table_name, f"{count:,}")
        # Si quedan filas por cargar, se cargan las que quedan en el buffer
        if buffer.tell() > 0:
            buffer.seek(0)
            cur.copy_expert(copy_sql, buffer)
            raw_conn.commit()

        logger.info("  %s — carga completa: %s filas totales.", table_name, f"{count:,}")
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        # Se resetean los parámetros de sesión orientados a cargas masivas por COPY.
        if cur is not None:
            cur.execute("RESET synchronous_commit")
            cur.execute("RESET maintenance_work_mem")
            cur.execute("RESET work_mem")
        raw_conn.close()


# -----------------------------------------------------------------------------------------------------
# Operaciones sobre la tabla de staging dwc_sql
# -----------------------------------------------------------------------------------------------------

def finalize_sql_table(db, old_name, new_name):
    # Renombra la columna v_scientificname y la tabla de staging dwc_sql a dwc_integrated
    # para mantener integridad del flujo definido en main.py.
    # Tras el RENAME, desactiva autovacuum en la relación (no es por tiempo: persiste hasta un ALTER SET true).
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - old_name: Nombre de la tabla de staging dwc_sql.
    # - new_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{old_name}" RENAME COLUMN "v_scientificname" TO "verbatimscientificname"'
        )
        conn.commit()
        logger.info(
            "Columna renombrada: v_scientificname a verbatimscientificname en %s",
            old_name,
        )

        if table_exists(db, new_name):
            logger.info("Ejecutando consulta")
            conn.execute(f'DROP TABLE "{new_name}"')
            conn.commit()
            logger.info("DROP TABLE existente: %s", new_name)
        logger.info("Ejecutando consulta")
        conn.execute(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"')
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{new_name}" SET (autovacuum_enabled = false)'
        )
        conn.commit()
    logger.info("Tabla SQL finalizada: %s → %s y autovacuum desactivado", old_name, new_name)

# -----------------------------------------------------------------------------------------------------
# Creación de índices en las tablas de staging dwc_occurrence y dwc_verbatim
# -----------------------------------------------------------------------------------------------------

def create_staging_indexes(db, table_names):
    # Crea un índice en la columna gbifID para facilitar el JOIN entre las tablas de staging.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_names: Diccionario con los nombres de las tablas de staging dwc_occurrence y dwc_verbatim.
    # Retorna:
    # - None: No retorna nada.
    with db.connect() as conn:
        for key in ('occurrence', 'verbatim'):
            tname = table_names[key]
            idx_name = f"idx_{tname}_gbifid"
            logger.info("Ejecutando consulta")
            conn.execute(f'CREATE INDEX "{idx_name}" ON "{tname}" ("gbifid")')
            conn.commit()
            logger.info("Indice creado: %s", idx_name)
        conn.commit()


# -----------------------------------------------------------------------------------------------------
# Creación de la tabla integrada dwc_occurrence_integrated desde las tablas de staging 
# -----------------------------------------------------------------------------------------------------

def create_integrated_table(db, table_names):
    # Se crea la tabla integrada dwc_occurrence_integrated desde las tablas de staging dwc_occurrence y dwc_verbatim
    # mediante un JOIN por la columna gbifID.
    # Es equivalente a ejecutar la siguiente consulta:
    # CREATE TABLE dwc_occurrence_integrated WITH (autovacuum_enabled = false) AS
    # SELECT o.*, v.*
    # FROM dwc_occurrence_fecha o
    # INNER JOIN dwc_verbatim_fecha v ON o.gbifID = v.gbifID;
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_names: Diccionario con los nombres de las tablas de staging dwc_occurrence y dwc_verbatim.
    # Retorna:
    # - None: No retorna nada.
    occurrence = table_names['occurrence']
    verbatim = table_names['verbatim']
    integrated = table_names['integrated']

    occurrence_cols = ', '.join(
        f'o."{c.lower()}"' for c in SIMPLE_COLS
    )
    verbatim_cols = ', '.join(
        f'v."{c.lower()}"' for c in OCURRENCE_COLS if c != 'gbifid'
    )
    with db.connect() as conn:
        if table_exists(db, integrated):
            logger.info("Ejecutando consulta")
            conn.execute(f'DROP TABLE "{integrated}"')
            conn.commit()
            logger.info("DROP TABLE existente: %s", integrated)

        sql = (
            f'CREATE TABLE "{integrated}" WITH (autovacuum_enabled = false) AS '
            f'SELECT {occurrence_cols}, {verbatim_cols} '
            f'FROM "{occurrence}" o '
            f'INNER JOIN "{verbatim}" v ON o."gbifid" = v."gbifid"'
        )
        logger.info("Ejecutando consulta")
        conn.execute(sql)
        conn.commit()
    logger.info("Tabla integrada creada: %s", integrated)

# -----------------------------------------------------------------------------------------------------
# Revisión de casos de nombres científicos vacíos en la tabla integrada
# -----------------------------------------------------------------------------------------------------

def fill_species_from_scientificname(db, table_name):
    # Se llena el campo species con las dos primeras palabras de scientificname cuando taxonrank es
    # SPECIES (valor típico de GBIF) y species es nulo o vacío.
    # Es equivalente a ejecutar la siguiente consulta:
    # UPDATE "dwc_integrated_{fecha}}" SET "species" = TRIM(split_part("scientificname", ' ', 1) || ' ' || split_part("scientificname", ' ', 2)) WHERE "taxonrank" = 'SPECIES' AND ("species" IS NULL OR TRIM("species") = '')
    # Parámetros:   
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        result = conn.execute(f"""
            UPDATE "{table_name}"
            SET "species" = TRIM(
                split_part("scientificname", ' ', 1) || ' ' ||
                split_part("scientificname", ' ', 2)
            )
            WHERE "taxonrank" = 'SPECIES'
            AND "scientificname" IS NOT NULL
            AND TRIM("scientificname") <> ''
            AND ("species" IS NULL OR TRIM("species") = '');
            """)
        conn.commit()
    logger.info("Campo species completado desde scientificname en %s (%s filas)", table_name, f"{result.rowcount:,}")


# -----------------------------------------------------------------------------------------------------
# Nombre de país en integrada (DwC country) a partir de countrycode
# -----------------------------------------------------------------------------------------------------

def normalize_integrated_country(db, table_name):
    # Añade si hace falta el campo country y lo rellena desde countrycode: ISO CO (insensible a mayúsculas
    # y espacios) → 'Colombia'; cualquier otro código o vacío → NULL. Actualización por lotes sobre gbifid
    # para tablas muy grandes (p. ej. decenas de millones de filas).
    integrated = table_name
    batch_size = UPDATE_BATCH_SIZE
    total = 0
    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "country" TEXT'
        )
        conn.commit()

        last_gbifid = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid '
                f'    FROM "{integrated}" i '
                f'    WHERE i."gbifid" > %s '
                f'    ORDER BY i."gbifid" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{integrated}" i SET '
                f'  "country" = CASE '
                f'    WHEN UPPER(BTRIM(COALESCE(i."countrycode", \'\'))) = \'CO\' '
                f"    THEN 'Colombia' "
                f'    ELSE NULL '
                f'  END '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid '
                f'RETURNING i."gbifid"',
                (last_gbifid,),
            )
            conn.commit()
            batch_n = result.rowcount
            id_rows = result.fetchall()
            if batch_n == 0:
                break
            last_gbifid = max(r[0] for r in id_rows)
            conn.commit()
            total += batch_n
            logger.info(
                "normalize_integrated_country batch en %s: %s filas (total %s, hasta gbifid=%s)",
                integrated,
                f"{batch_n:,}",
                f"{total:,}",
                last_gbifid,
            )
        logger.info(
            "Campo country normalizado en %s (%s filas; lotes de %s)",
            integrated,
            f"{total:,}",
            f"{batch_size:,}",
        )

    _run_table_maintenance(db, integrated)
    logger.info("VACUUM (ANALYZE) en %s tras actualización masiva de country", integrated)


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
        result = conn.execute(
            f'UPDATE "{integrated}" i '
            f'SET "taxonrank_id" = t."id" '
            f'FROM "taxonomic_taxon_rank" t '
            f'WHERE i."taxonrank" = t."taxonrank" '
            f'AND i."taxonrank_id" IS NULL '
            f'AND i."taxonrank" IS NOT NULL'
        )
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
            f'ALTER TABLE "{integrated}" '
            f'ADD CONSTRAINT "{fk_name}" '
            f'FOREIGN KEY ("taxonrank_id") '
            f'REFERENCES "taxonomic_taxon_rank" ("id") '
            f'ON UPDATE CASCADE '
            f'ON DELETE SET NULL '
            f'NOT VALID'
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
    # Prepara estructura base en la integrada (PK sobre gbifid).
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    integrated = table_name
    with db.connect() as conn:
        # Se agrega la Primary Key a la columna gbifid de la integrada
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" ADD PRIMARY KEY ("gbifid")'
        )
        conn.commit()
        logger.info("PK a campo gbifid agregada a %s", integrated)
        conn.commit()

# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces espaciales con la tabla MGN_ADM_MPIO_2025 (división político-administrativa) e Invemar_maritime_regions (regiones marítimas)
# --------------------------------------------------------------------------------------------------------------------------------------

# Palabras que se deben convertir a minúsculas después de INITCAP en los campos de departamento y municipio
# para estandarización de nombres. Por ejemplo, 'Norte De Santander' a 'Norte de Santander'.
_LOWERCASE_WORDS = (' De ', ' Y ', ' Del ', ' La ')

def spatials_joins(db, table_name):
    # Cruza la tabla geo_locality_validation con MGN_ADM_MPIO_2025 e INVEMAR_MARITIME_REGIONS usando ST_Intersects
    # y aplica INITCAP a los campos de departamento y municipio para estandarización de nombres.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated (se mantiene por compatibilidad).
    # Retorna:
    # - None: No retorna nada.
    batch_size = UPDATE_BATCH_SIZE
    with db.connect() as conn:
        total_mgn = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "geo_locality_validation" '
                f'    WHERE geom IS NOT NULL '
                f'      AND "stateprovincemgn" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "geo_locality_validation" i '
                f'SET "stateprovincemgn" = m."dpto_cnmbr", '
                f'    "countymgn" = m."mpio_cnmbr" '
                f'FROM batch b, "MGN_ADM_MPIO_2025" m '
                f'WHERE i.ctid = b.ctid '
                f'AND ST_Intersects(i.geom, m.geom)'
            )
            conn.commit()
            batch_updated = result.rowcount
            conn.commit()
            if batch_updated == 0:
                break
            total_mgn += batch_updated
            logger.info("Cruce MGN en batch en %s: %s filas (total %s)", "geo_locality_validation", f"{batch_updated:,}", f"{total_mgn:,}")
        logger.info("Cruce espacial con MGN_ADM_MPIO_2025 completado en %s (%s filas)", "geo_locality_validation", f"{total_mgn:,}")

        total_invemar = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "geo_locality_validation" '
                f'    WHERE geom IS NOT NULL '
                f'      AND "countymgn" IS NULL '
                f'      AND "maritimeregion" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "geo_locality_validation" i '
                f'SET "maritimeregion" = m."DESCRIP" '
                f'FROM batch b, "INVEMAR_MARITIME_REGIONS" m '
                f'WHERE i.ctid = b.ctid '
                f'AND ST_Intersects(i.geom, m.geom)'
            )
            conn.commit()
            batch_updated = result.rowcount
            conn.commit()
            if batch_updated == 0:
                break
            total_invemar += batch_updated
            logger.info("Cruce INVEMAR en batch en %s: %s filas (total %s)", "geo_locality_validation", f"{batch_updated:,}", f"{total_invemar:,}")
        logger.info("Cruce espacial con INVEMAR_MARITIME_REGIONS completado en %s (%s filas)", "geo_locality_validation", f"{total_invemar:,}")

        total_narino = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "geo_locality_validation" '
                f'    WHERE geom IS NOT NULL '
                f'      AND "narinomaritimeregion" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "geo_locality_validation" i '
                f'SET "narinomaritimeregion" = m."Nombre" '
                f'FROM batch b, "NARINO_MARITIME_REGION" m '
                f'WHERE i.ctid = b.ctid '
                f'AND ST_Intersects(i.geom, m.geom)'
            )
            conn.commit()
            batch_updated = result.rowcount
            conn.commit()
            if batch_updated == 0:
                break
            total_narino += batch_updated
            logger.info("Cruce Nariño en batch en %s: %s filas (total %s)", "geo_locality_validation", f"{batch_updated:,}", f"{total_narino:,}")
        logger.info("Cruce espacial con NARINO_MARITIME_REGION completado en %s (%s filas)", "geo_locality_validation", f"{total_narino:,}")

        # Se aplica INITCAP a los campos de departamento y municipio para estandarización de nombres.
        for col in ('stateprovincemgn', 'countymgn'):
            expr = f'INITCAP("{col}")'
            # Se reemplazan las palabras que se deben convertir a minúsculas después de INITCAP en los campos de departamento y municipio
            # Cada palabra en _LOWERCASE_WORDS se formatea para que sea un replace en SQL.
            for word in _LOWERCASE_WORDS:
                expr = f"REPLACE({expr}, '{word}', '{word.lower()}')"

            logger.info("Ejecutando consulta")
            conn.execute(
                f'UPDATE "geo_locality_validation" SET "{col}" = {expr} '
                f'WHERE "{col}" IS NOT NULL '
                f'AND "{col}" IS DISTINCT FROM {expr}'
            )
            conn.commit()

            logger.info("INITCAP con estandarizaciones de nombres aplicado a %s en %s", col, "geo_locality_validation")

        # Reemplazos manuales para mantener consistencia con la salida de sintesis de cifras de biodiversidad
        # Bogotá, D.C. -> Bogotá, D. C.
        # Santiago de Cali -> Cali

        logger.info("Ejecutando consulta")
        conn.execute(
            f'UPDATE "geo_locality_validation" '
            f'SET "stateprovincemgn" = \'Bogotá, D. C.\', '
            f'    "countymgn" = \'Bogotá, D. C.\' '
            f'WHERE "stateprovincemgn" = \'Bogotá, D.C.\' '
        )
        conn.commit()

        logger.info("Reemplazos manuales para mantener consistencia con la salida de sintesis de cifras de biodiversidad completados en %s", "geo_locality_validation")

# -----------------------------------------------------------------------------------------------------
# Normalización de stateprovince y county en geo_locality_validation
# -----------------------------------------------------------------------------------------------------

def normalize_stateprovince_county(db, table_name):
    # Normaliza stateprovince y county en geo_locality_validation antes de validar geografía.
    # Las validaciones de departamento se ejecutan por lotes (ctid + LIMIT) para acotar WAL y locks.
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated (se mantiene por compatibilidad).
    # Retorna:
    # - None: No retorna nada.
    _ = table_name  # firma mantenida para compatibilidad con el orquestador (main.timer).
    locality = 'geo_locality_validation'
    batch_size = UPDATE_BATCH_SIZE
    with db.connect() as conn:
        # Validación 1: Stateprovincevalidated desde geo_stateprovince_validation (solo NULL).
        total_v1 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH candidates AS ('
                f'    SELECT DISTINCT i.ctid '
                f'    FROM "{locality}" i '
                f'    INNER JOIN "geo_stateprovince_validation" a '
                f'      ON UPPER(TRIM(i."stateprovince")) = UPPER(TRIM(a."originalstateprovince")) '
                f'    INNER JOIN "geo_master_geography" d ON d."id" = a."geo_master_geography_id" '
                f'    WHERE i."stateprovincevalidated" IS NULL '
                f'      AND a."geo_master_geography_id" IS NOT NULL'
                f'), batch AS ('
                f'    SELECT ctid FROM candidates LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "stateprovincevalidated" = TRIM(d."name") '
                f'FROM batch b, "geo_stateprovince_validation" a, "geo_master_geography" d '
                f'WHERE i.ctid = b.ctid '
                f'AND UPPER(TRIM(i."stateprovince")) = UPPER(TRIM(a."originalstateprovince")) '
                f'AND d."id" = a."geo_master_geography_id"'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_v1 += n
            logger.info(
                "Validación 1 (alias departamento) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_v1:,}",
            )
        logger.info(
            "Validación 1 (alias departamento) completada en %s (%s filas)",
            locality,
            f"{total_v1:,}",
        )

        # Validación 2: región marina Nariño → stateprovincevalidated = Nariño.
        total_v2 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "{locality}" '
                f'    WHERE BTRIM(COALESCE("narinomaritimeregion", \'\')) <> \'\' '
                f'      AND "stateprovincevalidated" IS DISTINCT FROM \'Nariño\' '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" t '
                f'SET "stateprovincevalidated" = \'Nariño\' '
                f'FROM batch b '
                f'WHERE t.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_v2 += n
            logger.info(
                "Validación 2 (Nariño marítimo) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_v2:,}",
            )
        logger.info(
            "Validación 2 (Nariño marítimo) completada en %s (%s filas)",
            locality,
            f"{total_v2:,}",
        )

        # Validación 3: copia MGN a validado sólo si falta verbatim (stateprovince IS NULL).
        total_v3 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "{locality}" '
                f'    WHERE "stateprovincemgn" IS NOT NULL '
                f'      AND "stateprovincevalidated" IS NULL '
                f'      AND "stateprovince" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "stateprovincevalidated" = TRIM(i."stateprovincemgn") '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_v3 += n
            logger.info(
                "Validación 3 (MGN) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_v3:,}",
            )
        logger.info(
            "Validación 3 (MGN) completada en %s (%s filas)",
            locality,
            f"{total_v3:,}",
        )

        # Validación 1 (municipio): county original + catálogo de alias -> countyvalidated.
        # Solo aplica cuando county en locality no es NULL.
        total_county_v1 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH candidates AS ('
                f'    SELECT DISTINCT i.ctid '
                f'    FROM "{locality}" i '
                f'    INNER JOIN "geo_county_validation" c '
                f'      ON UPPER(TRIM(i."county")) = UPPER(TRIM(c."originalcounty")) '
                f'    WHERE i."countyvalidated" IS NULL '
                f'      AND i."county" IS NOT NULL '
                f'      AND c."revisedcounty" IS NOT NULL'
                f'), batch AS ('
                f'    SELECT ctid FROM candidates LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "countyvalidated" = TRIM(c."revisedcounty") '
                f'FROM batch b, "geo_county_validation" c '
                f'WHERE i.ctid = b.ctid '
                f'AND UPPER(TRIM(i."county")) = UPPER(TRIM(c."originalcounty"))'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_county_v1 += n
            logger.info(
                "Validación 1 (alias municipio) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_county_v1:,}",
            )
        logger.info(
            "Validación 1 (alias municipio) completada en %s (%s filas)",
            locality,
            f"{total_county_v1:,}",
        )

        # Validación 2 (municipio): Valida con MGN cuando countyvalidated es NULL o vacío.
        total_county_v2 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "{locality}" '
                f'    WHERE "countymgn" IS NOT NULL '
                f'      AND "countyvalidated" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "countyvalidated" = TRIM(i."countymgn") '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_county_v2 += n
            logger.info(
                "Validación 2 (MGN municipio) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_county_v2:,}",
            )
        logger.info(
            "Validación 2 (MGN municipio) completada en %s (%s filas)",
            locality,
            f"{total_county_v2:,}",
        )

        # Validación 3 (municipio): pareja (stateprovincevalidated, countyvalidated) debe existir en DIVIPOLA.
        # Dos pasos: (3a) sustituir por countymgn si MGN coincide con depto validado y la pareja MGN es válida;
        # (3b) NULL al resto con pareja inválida. Evita CASE + EXISTS duplicado y el bucle infinito.
        _invalid_county_pair = (
            f'NULLIF(BTRIM(i."countyvalidated"), \'\') IS NOT NULL '
            f'AND NOT EXISTS ('
            f'    SELECT 1 FROM "geo_master_geography" m '
            f'    INNER JOIN "geo_master_geography" d ON d."id" = m."parent_id" '
            f'    WHERE m."subtype" = \'municipio\' '
            f'      AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countyvalidated")) '
            f'      AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated"))'
            f')'
        )
        total_county_v3 = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid '
                f'    FROM "{locality}" i '
                f'    INNER JOIN "geo_master_geography" m '
                f'      ON m."subtype" = \'municipio\' '
                f'     AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countymgn")) '
                f'    INNER JOIN "geo_master_geography" d '
                f'      ON d."id" = m."parent_id" '
                f'     AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated")) '
                f'    WHERE {_invalid_county_pair} '
                f'      AND NULLIF(BTRIM(i."countymgn"), \'\') IS NOT NULL '
                f'      AND UPPER(TRIM(COALESCE(i."stateprovincemgn", \'\'))) = '
                f'          UPPER(TRIM(COALESCE(i."stateprovincevalidated", \'\'))) '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "countyvalidated" = TRIM(i."countymgn") '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_county_v3 += n
            logger.info(
                "Validación 3a en batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_county_v3:,}",
            )
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid '
                f'    FROM "{locality}" i '
                f'    WHERE {_invalid_county_pair} '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "countyvalidated" = NULL '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_county_v3 += n
            logger.info(
                "Validación 3b (limpiar pareja inválida) batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_county_v3:,}",
            )
        logger.info(
            "Validación 3 (consistencia depto/municipio) completada en %s (%s filas)",
            locality,
            f"{total_county_v3:,}",
        )
# -----------------------------------------------------------------------------------------------------
# Cruce final con DIVIPOLA: asignación de geo_master_geography_id desde stateprovincevalidated/countyvalidated.
# -----------------------------------------------------------------------------------------------------

        # Caso 1: stateprovincevalidated + countyvalidated -> municipio y su parent_id (departamento).
        total_geo_divipola_municipio = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid, m."id" AS geo_master_geography_id '
                f'    FROM "{locality}" i '
                f'    INNER JOIN "geo_master_geography" m '
                f'      ON m."subtype" = \'municipio\' '
                f'     AND UPPER(TRIM(m."name")) = UPPER(TRIM(i."countyvalidated")) '
                f'    INNER JOIN "geo_master_geography" d '
                f'      ON d."id" = m."parent_id" '
                f'     AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated")) '
                f'    WHERE NULLIF(BTRIM(i."stateprovincevalidated"), \'\') IS NOT NULL '
                f'      AND NULLIF(BTRIM(i."countyvalidated"), \'\') IS NOT NULL '
                f'      AND i."geo_master_geography_id" IS DISTINCT FROM m."id" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "geo_master_geography_id" = b.geo_master_geography_id '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_geo_divipola_municipio += n
            logger.info(
                "Cruce DIVIPOLA municipio batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_geo_divipola_municipio:,}",
            )
        logger.info(
            "Cruce DIVIPOLA municipio completado en %s (%s filas)",
            locality,
            f"{total_geo_divipola_municipio:,}",
        )

        # Caso 2: solo stateprovincevalidated (countyvalidated nulo/vacío) -> departamento.
        total_geo_divipola_departamento = 0
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid, d."id" AS geo_master_geography_id '
                f'    FROM "{locality}" i '
                f'    INNER JOIN "geo_master_geography" d '
                f'      ON d."subtype" = \'departamento\' '
                f'     AND UPPER(TRIM(d."name")) = UPPER(TRIM(i."stateprovincevalidated")) '
                f'    WHERE NULLIF(BTRIM(i."stateprovincevalidated"), \'\') IS NOT NULL '
                f'      AND NULLIF(BTRIM(i."countyvalidated"), \'\') IS NULL '
                f'      AND i."geo_master_geography_id" IS DISTINCT FROM d."id" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality}" i '
                f'SET "geo_master_geography_id" = b.geo_master_geography_id '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            n = result.rowcount
            conn.commit()
            if n == 0:
                break
            total_geo_divipola_departamento += n
            logger.info(
                "Cruce DIVIPOLA departamento batch en %s: %s filas (total %s)",
                locality,
                f"{n:,}",
                f"{total_geo_divipola_departamento:,}",
            )
        logger.info(
            "Cruce DIVIPOLA departamento completado en %s (%s filas)",
            locality,
            f"{total_geo_divipola_departamento:,}",
        )

    logger.info(
        "Normalización de stateprovince/county y slugs completada en %s",
        locality,
    )

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
        conn.execute(
            f'CREATE TABLE "{species_tbl}" ('
            f'  "id" SERIAL PRIMARY KEY, '
            f'  "kingdom" TEXT, '
            f'  "phylum" TEXT, '
            f'  "class" TEXT, '
            f'  "order" TEXT, '
            f'  "family" TEXT, '
            f'  "genus" TEXT, '
            f'  "species" TEXT NOT NULL, '
            f'  "cites" TEXT, '
            f'  "threatstatusuicn" TEXT, '
            f'  "threatstatusmads" TEXT, '
            f'  "exotic" TEXT, '
            f'  "exoticriskinvasion" TEXT, '
            f'  "invasiveness" TEXT, '
            f'  "invasive" TEXT, '
            f'  "transplanted" TEXT, '
            f'  "migratory" TEXT, '
            f'  "endemic" TEXT, '
            f'  "referencelist" TEXT, '
            f'  "flagtaxo" TEXT, '
            f'  CONSTRAINT "uq_{species_tbl}_species" UNIQUE ("species")'
            f')'
        )
        conn.commit()
        logger.info("Tabla de validación taxonómica por especie creada: %s", species_tbl)

        logger.info("Ejecutando consulta")
        conn.execute(
            f'INSERT INTO "{species_tbl}" ('
            f'  "kingdom", "phylum", "class", "order", "family", '
            f'  "genus", "species"'
            f') '
            f'SELECT DISTINCT ON (i."species") '
            f'  i."kingdom", i."phylum", i."class", i."order", i."family", '
            f'  i."genus", i."species" '
            f'FROM "{integrated}" i '
            f'WHERE i."species" IS NOT NULL AND BTRIM(i."species") <> \'\' '
            f'ORDER BY i."species", i."gbifid"'
        )
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
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{pending_idx}" '
            f'ON "{integrated}" ("species") '
            f'WHERE "taxonomic_species_id" IS NULL '
            f'  AND "species" IS NOT NULL '
            f'  AND BTRIM("species") <> \'\''
        )
        conn.commit()
        logger.info("Indice parcial creado: %s", pending_idx)

        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid, s.id AS taxonomic_species_id '
                f'    FROM "{integrated}" i '
                f'    INNER JOIN "{species_tbl}" s ON i."species" = s."species" '
                f'    WHERE i."taxonomic_species_id" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{integrated}" i '
                f'SET "taxonomic_species_id" = b.taxonomic_species_id '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
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

    _run_table_maintenance(db, integrated)
    logger.info(
        "VACUUM (ANALYZE) en %s tras enlace taxonomic_species_id",
        integrated,
    )


# --------------------------------------------------------------------------------------------------------------------------------------
# Tabla de localidades únicas y referencia desde la integrada
# --------------------------------------------------------------------------------------------------------------------------------------

def validate_localities(db, table_name):
    # Mantiene una tabla de localidades únicas a partir de los campos
    # decimallatitude, decimallongitude, county, stateprovince, e inserta
    # nuevas combinaciones desde la tabla integrada actual y completa geom.
    # El enlace masivo locality_id en la integrada: link_integrated_locality_id (main al final).
    # Parámetros:
    # - db: Conexión al pool de conexiones de PostgreSQL.
    # - table_name: Nombre de la tabla integrada dwc_integrated.
    # Retorna:
    # - None: No retorna nada.
    integrated = table_name
    batch_size = SQL_BATCH_SIZE
    total_updated = 0

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "locality_id" INT4'
        )
        conn.commit()
        logger.info("Columna locality_id creada en %s", integrated)
        # Inserta combinaciones nuevas desde integrated (orden = índice único en BD).
        logger.info("Ejecutando consulta")
        conn.execute(
            f'INSERT INTO "geo_locality_validation" '
            f'("decimallatitude", "decimallongitude", "stateprovince", "county") '
            f'SELECT DISTINCT '
            f'i."decimallatitude", '
            f'i."decimallongitude", '
            f'i."stateprovince", '
            f'i."county" '
            f'FROM "{integrated}" i '
            f'WHERE NOT EXISTS ('
            f'    SELECT 1 '
            f'    FROM "geo_locality_validation" l '
            f'    WHERE l."decimallatitude" IS NOT DISTINCT FROM i."decimallatitude" '
            f'      AND l."decimallongitude" IS NOT DISTINCT FROM i."decimallongitude" '
            f'      AND l."stateprovince" IS NOT DISTINCT FROM i."stateprovince" '
            f'      AND l."county" IS NOT DISTINCT FROM i."county"'
            f') '
            f'ON CONFLICT ("decimallatitude", "decimallongitude", "stateprovince", "county") DO NOTHING'
        )
        conn.commit()
        logger.info("Combinaciones nuevas de integrada insertadas en %s: geo_locality_validation", integrated)
        # Completa geom por lotes solo cuando falta geometría y hay coordenadas.
        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT ctid '
                f'    FROM "geo_locality_validation" '
                f'    WHERE "geom" IS NULL '
                f'      AND "decimallatitude" IS NOT NULL '
                f'      AND "decimallongitude" IS NOT NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "geo_locality_validation" t '
                f'SET "geom" = ST_SetSRID(ST_MakePoint(t."decimallongitude", t."decimallatitude"), 4326) '
                f'FROM batch '
                f'WHERE t.ctid = batch.ctid'
            )
            conn.commit()
            batch_updated = result.rowcount
            conn.commit()
            if batch_updated == 0:
                break
            total_updated += batch_updated
            logger.info(
                "Geom batch en %s: %s filas (total %s)",
                "geo_locality_validation",
                f"{batch_updated:,}",
                f"{total_updated:,}",
            )
        conn.commit()
        logger.info(
            "geo_locality_validation actualizada desde %s (geom=%s; enlace en link_integrated_locality_id)",
            integrated,
            f"{total_updated:,}",
        )


def link_integrated_locality_id(db, table_name):
    # Propaga id de geo_locality_validation (~2M) a la integrada (~40M) por lat/lon/state/county.
    # CTE + LIMIT por lote (SQL_BATCH_SIZE / FLUSH_EVERY). Índice parcial en integrada solo filas
    # con locality_id IS NULL. El catálogo ya tiene UNIQUE en los 4 campos para el JOIN.
    integrated = table_name
    locality_tbl = 'geo_locality_validation'
    fk_name = f"fk_{integrated}_locality_id"
    pending_idx = f'idx_{integrated}_locality_link_pending'
    batch_size = SQL_BATCH_SIZE
    total_linked = 0

    with db.connect() as conn:
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "locality_id" INT4'
        )
        conn.commit()

        logger.info("Ejecutando consulta")
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{pending_idx}" '
            f'ON "{integrated}" ('
            f'  "decimallatitude", "decimallongitude", "stateprovince", "county"'
            f') '
            f'WHERE "locality_id" IS NULL'
        )
        conn.commit()
        logger.info("Indice parcial creado: %s", pending_idx)

        while True:
            logger.info("Ejecutando consulta")
            result = conn.execute(
                f'WITH batch AS ('
                f'    SELECT i.ctid, l."id" AS locality_id '
                f'    FROM "{integrated}" i '
                f'    INNER JOIN "{locality_tbl}" l '
                f'      ON i."decimallatitude" IS NOT DISTINCT FROM l."decimallatitude" '
                f'     AND i."decimallongitude" IS NOT DISTINCT FROM l."decimallongitude" '
                f'     AND i."stateprovince" IS NOT DISTINCT FROM l."stateprovince" '
                f'     AND i."county" IS NOT DISTINCT FROM l."county" '
                f'    WHERE i."locality_id" IS NULL '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{integrated}" i '
                f'SET "locality_id" = b.locality_id '
                f'FROM batch b '
                f'WHERE i.ctid = b.ctid'
            )
            conn.commit()
            batch_linked = result.rowcount
            conn.commit()
            if batch_linked == 0:
                break
            total_linked += batch_linked
            logger.info(
                "locality_id batch en %s: %s filas (total %s)",
                integrated,
                f"{batch_linked:,}",
                f"{total_linked:,}",
            )

        logger.info("Ejecutando consulta")
        conn.execute(f'DROP INDEX IF EXISTS "{pending_idx}"')
        conn.commit()

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
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD CONSTRAINT "{fk_name}" '
            f'FOREIGN KEY ("locality_id") '
            f'REFERENCES "geo_locality_validation" ("id") '
            f'ON UPDATE CASCADE '
            f'ON DELETE SET NULL '
            f'NOT VALID'
        )
        conn.commit()
        logger.info(
            "Enlace integrada → geo_locality_validation: %s filas con locality_id",
            f"{total_linked:,}",
        )

    _run_table_maintenance(db, integrated)
    logger.info(
        "VACUUM (ANALYZE) en %s tras locality_id y FK",
        integrated,
    )


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
                f'WITH batch AS ('
                f'    SELECT t.ctid '
                f'    FROM "{locality_tbl}" t '
                f'    WHERE t."id" > %s '
                f'    ORDER BY t."id" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality_tbl}" t SET '
                f'"stateprovincevalidation" = CASE '
                f'WHEN NULLIF(BTRIM(t."stateprovincevalidated"), \'\') IS NULL THEN NULL '
                f'WHEN UPPER(TRIM(t."stateprovincevalidated")) = '
                f'     UPPER(TRIM(COALESCE(t."stateprovincemgn", \'\'))) THEN TRUE '
                f'WHEN (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL) '
                f'     OR (COALESCE(t."decimallatitude", 0) = 0 AND COALESCE(t."decimallongitude", 0) = 0) '
                f'     THEN NULL '
                f'WHEN NULLIF(BTRIM(t."maritimeregion"), \'\') IS NOT NULL THEN NULL '
                f'ELSE FALSE END '
                f'FROM batch b '
                f'WHERE t.ctid = b.ctid '
                f'RETURNING t."id"',
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
                f'WITH batch AS ('
                f'    SELECT t.ctid '
                f'    FROM "{locality_tbl}" t '
                f'    WHERE t."id" > %s '
                f'    ORDER BY t."id" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality_tbl}" t SET '
                f'"countyvalidation" = CASE '
                f'WHEN NULLIF(BTRIM(t."countyvalidated"), \'\') IS NULL THEN NULL '
                f'WHEN UPPER(TRIM(t."countyvalidated")) = '
                f'     UPPER(TRIM(COALESCE(t."countymgn", \'\'))) THEN TRUE '
                f'WHEN (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL) '
                f'     OR (COALESCE(t."decimallatitude", 0) = 0 AND COALESCE(t."decimallongitude", 0) = 0) '
                f'     THEN NULL '
                f'WHEN NULLIF(BTRIM(t."maritimeregion"), \'\') IS NOT NULL THEN NULL '
                f'ELSE FALSE END '
                f'FROM batch b '
                f'WHERE t.ctid = b.ctid '
                f'RETURNING t."id"',
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
                f'WITH batch AS ('
                f'    SELECT t.ctid '
                f'    FROM "{locality_tbl}" t '
                f'    WHERE t."id" > %s '
                f'    ORDER BY t."id" '
                f'    LIMIT {batch_size}'
                f') '
                f'UPDATE "{locality_tbl}" t SET '
                f'"flaggeo" = CASE '
                f'WHEN t."stateprovincevalidation" IS FALSE AND t."countyvalidation" IS FALSE '
                f"THEN 'Departamento y municipio no coinciden con ubicación de la coordenada' "
                f'WHEN t."stateprovincevalidation" IS TRUE AND t."countyvalidation" IS FALSE '
                f"THEN 'Municipio no coincide con ubicación de la coordenada' "
                f'WHEN t."stateprovincevalidation" IS FALSE AND t."countyvalidation" IS TRUE '
                f"THEN 'Departamento no coincide con ubicación de la coordenada' "
                f'WHEN t."stateprovincevalidation" IS NULL AND t."countyvalidation" IS NULL '
                f'AND NULLIF(BTRIM(t."maritimeregion"), \'\') IS NOT NULL '
                f"THEN 'Coordenada en área marítima' "
                f'WHEN t."stateprovincevalidation" IS NULL AND t."countyvalidation" IS NULL '
                f'AND (t."decimallatitude" IS NULL AND t."decimallongitude" IS NULL '
                f'     OR (COALESCE(t."decimallatitude", 0) = 0 AND COALESCE(t."decimallongitude", 0) = 0)) '
                f"THEN 'Sin coordenadas' "
                f'ELSE NULL END '
                f'FROM batch b '
                f'WHERE t.ctid = b.ctid '
                f'RETURNING t."id"',
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
    logger.info(
        "VACUUM (ANALYZE) en %s tras validación geográfica (tabla de localidades)",
        locality_tbl,
    )

# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces taxonómicos con listados de referencia
# --------------------------------------------------------------------------------------------------------------------------------------

# Se definen las tablas y los campos a cruzar. La idea es iterar sobre las tablas y campos para evitar
# tener que definirlas las consultas SQL manualmente.
# Los cruces actualizan taxonomic_species_validation (v) por species; la integrada enlaza por taxonomic_species_id.
# Es equivalente a ejecutar la siguiente consulta:
# UPDATE "taxonomic_species_validation" v SET "cites" = t."cites" FROM "taxonomic_cites" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "threatstatusuicn" = t."threatstatus" FROM "taxonomic_threat_uicn" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "threatstatusmads" = t."threatstatus" FROM "taxonomic_threat_mads" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "exotic" = t."exotic", ... FROM "taxonomic_invasive_exotic" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "migratory" = t."migratory", "endemic" = t."endemic" FROM "taxonomic_col_list" t WHERE v."species" = t."species"
# UPDATE "taxonomic_species_validation" v SET "referencelist" = t."datasetid" FROM "taxonomic_col_list" t WHERE v."species" = t."species"
_FLAGTAXO_CLASSES = ('Aves', 'Mammalia', 'Reptilia', 'Squamata', 'Crocodylia', 'Testudines')
_FLAGTAXO_ORDERS = ('Lepidoptera','Odonota')

_TAXONOMIC_JOINS = {
    'taxonomic_cites': {
        'columns': {'cites': 'cites'},
    },
    'taxonomic_threat_uicn': {
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
}


def taxonomic_joins(db, table_name):
    # Cruza taxonomic_species_validation con tablas taxonómicas por el campo species.
    # table_name se conserva por compatibilidad con el orquestador.
    species_tbl = 'taxonomic_species_validation'
    with db.connect() as conn:
        for src_table, config in _TAXONOMIC_JOINS.items():
            col_map = config['columns']

            # migratory solo se completa si está nulo, para no depender del orden
            # entre taxonomic_migratory y taxonomic_col_list.
            set_parts = []
            for src, dest in col_map.items():
                if dest == 'migratory':
                    set_parts.append(
                        f'"{dest}" = CASE '
                        f'WHEN v."migratory" IS NULL THEN t."{src}" '
                        f'ELSE v."migratory" END'
                    )
                else:
                    set_parts.append(f'"{dest}" = t."{src}"')
            set_clause = ', '.join(set_parts)
            logger.info("Ejecutando consulta")
            conn.execute(
                f'UPDATE "{species_tbl}" v '
                f'SET {set_clause} '
                f'FROM "{src_table}" t '
                f'WHERE v."species" = t."species"'
            )
            conn.commit()
            logger.info("Join con %s completado en %s", src_table, species_tbl)

        logger.info("Ejecutando consulta")
        conn.execute(
            f'UPDATE "{species_tbl}" '
            f"SET \"referencelist\" = 'Presente en lista taxonómica: ' || \"referencelist\" "
            f'WHERE "referencelist" IS NOT NULL'
        )
        conn.commit()
        logger.info("Campo referencelist actualizado en %s", species_tbl)

        classes_list = ', '.join(f"'{c}'" for c in _FLAGTAXO_CLASSES)
        orders_list = ', '.join(f"'{o}'" for o in _FLAGTAXO_ORDERS)

        logger.info("Ejecutando consulta")
        conn.execute(
            f'UPDATE "{species_tbl}" SET "flagtaxo" = CASE '
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"transplanted\" = 'Trasplantada' "
            f"THEN 'Ausente en lista taxonómica_Trasplantada' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"migratory\" = 'Migratorio' "
            f"THEN 'Ausente en lista taxonómica_Migratoria' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"exoticriskinvasion\" = 'Exótica con potencial de invasión' "
            f"THEN 'Ausente en lista taxonómica_Exótica con potencial de invasión' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"invasive\" = 'Invasora' "
            f"THEN 'Ausente en lista taxonómica_Invasora' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"exotic\" = 'Exótica' "
            f"THEN 'Ausente en lista taxonómica_Exótica' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f'AND "class" IN ({classes_list}) '
            f"THEN 'Ausente en lista taxonómica' "
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f'AND "order" IN ({orders_list}) '
            f"THEN 'Ausente en lista taxonómica' "
            f'ELSE NULL END'
        )
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
        conn.execute(
            f'UPDATE "{species_tbl}" '
            f'SET "threatstatusuicn" = NULLIF(TRIM("threatstatusuicn"), \'\'), '
            f'    "threatstatusmads" = NULLIF(TRIM("threatstatusmads"), \'\') '
            f'WHERE "threatstatusuicn" IS NOT NULL OR "threatstatusmads" IS NOT NULL'
        )
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f'UPDATE "{species_tbl}" '
            f'SET "threatstatusuicn" = CASE '
            f'    WHEN "threatstatusuicn" IS NULL THEN NULL '
            f'    WHEN "threatstatusuicn" LIKE \'%_IUCN\' THEN "threatstatusuicn" '
            f'    ELSE "threatstatusuicn" || \'_IUCN\' '
            f'END, '
            f'    "threatstatusmads" = CASE '
            f'    WHEN "threatstatusmads" IS NULL THEN NULL '
            f'    WHEN "threatstatusmads" LIKE \'%_MADS\' THEN "threatstatusmads" '
            f'    ELSE "threatstatusmads" || \'_MADS\' '
            f'END'
        )
        conn.commit()
        logger.info(
            "Validación de threatstatus (vacíos/sufijos por fuente) completada en %s (integrada: %s)",
            species_tbl,
            table_name,
        )
        conn.commit()

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


def gbif_api_calls(db, table_name):
    # Completa gbif_datasets y gbif_publishers desde tablas locales y API GBIF. La integrada solo
    # aporta datasetkey y publishingorgkey; los campos descriptivos se leen con JOIN en consultas.
    # Al final añade FK NOT VALID hacia gbif_datasets y gbif_publishers (claves TEXT). Las filas con
    # clave NULL no participan en la FK. Huérfanos existentes siguen permitidos hasta
    # ALTER TABLE ... VALIDATE CONSTRAINT ...; nuevas filas ya se validan contra el catálogo actual.
    integrated = table_name
    fk_dataset = f"fk_{integrated}_gbif_datasetkey"
    fk_publisher = f"fk_{integrated}_gbif_publishingorgkey"
    missing_dataset_keys = []
    missing_publisher_keys = []
    with db.connect() as conn:
        dataset_upsert_sql = """
            INSERT INTO gbif_datasets (datasetkey, license, doi, datasettitle, logourl, datatype, created)
            VALUES (%(datasetkey)s, %(license)s, %(doi)s, %(datasettitle)s, %(logourl)s, %(datatype)s, %(created)s)
            ON CONFLICT (datasetkey) DO UPDATE
            SET license = EXCLUDED.license,
                doi = EXCLUDED.doi,
                datasettitle = EXCLUDED.datasettitle,
                logourl = EXCLUDED.logourl,
                datatype = EXCLUDED.datatype,
                created = EXCLUDED.created
        """

        # Datasetkeys presentes en la integrada sin fila en catálogo o sin título en gbif_datasets.
        logger.info("Ejecutando consulta")
        dataset_rows = conn.execute(
            f'SELECT DISTINCT i."datasetkey" '
            f'FROM "{integrated}" i '
            f'LEFT JOIN "gbif_datasets" d ON i."datasetkey" = d."datasetkey" '
            f'WHERE i."datasetkey" IS NOT NULL '
            f'  AND (d."datasetkey" IS NULL OR d."datasettitle" IS NULL)'
        ).fetchall()
        conn.commit()
        missing_dataset_keys = [row[0] for row in dataset_rows if row[0]]
        logger.info(
            "Datasetkeys sin datasettitle en %s: %s",
            integrated,
            f"{len(missing_dataset_keys):,}",
        )

        ds_fetched = 0
        ds_upserted = 0
        ds_errors = 0
        if missing_dataset_keys:
            max_workers = min(20, max(4, len(missing_dataset_keys)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        _fetch_gbif_json,
                        f'https://api.gbif.org/v1/dataset/{key}',
                        key,
                        'datasetkey',
                    ): key
                    for key in missing_dataset_keys
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        data, ok = future.result()
                    except Exception as e:
                        logger.warning("Error ejecutando tarea GBIF para datasetkey %s: %s", key, e)
                        ds_errors += 1
                        continue

                    if not ok:
                        ds_errors += 1
                        continue

                    ds_fetched += 1
                    logger.info("Ejecutando consulta")
                    conn.execute(dataset_upsert_sql, {
                        'datasetkey': data.get('key') or key,
                        'license': data.get('license'),
                        'doi': data.get('doi'),
                        'datasettitle': data.get('title'),
                        'logourl': data.get('logoUrl'),
                        'datatype': data.get('type'),
                        'created': _parse_gbif_created_date(data.get('created')),
                    })
                    conn.commit()
                    ds_upserted += 1
                    time.sleep(0.002)

        publisher_upsert_sql = """
            INSERT INTO gbif_publishers (publishingorgkey, organization, institutionid)
            VALUES (%(publishingorgkey)s, %(organization)s, %(institutionid)s)
            ON CONFLICT (publishingorgkey) DO UPDATE
            SET organization = EXCLUDED.organization,
                institutionid = EXCLUDED.institutionid
        """

        # Publishingorgkeys en la integrada sin fila en catálogo o sin organization en gbif_publishers.
        logger.info("Ejecutando consulta")
        publisher_rows = conn.execute(
            f'SELECT DISTINCT i."publishingorgkey" '
            f'FROM "{integrated}" i '
            f'LEFT JOIN "gbif_publishers" p ON i."publishingorgkey" = p."publishingorgkey" '
            f'WHERE i."publishingorgkey" IS NOT NULL '
            f'  AND (p."publishingorgkey" IS NULL OR p."organization" IS NULL)'
        ).fetchall()
        conn.commit()
        missing_publisher_keys = [row[0] for row in publisher_rows if row[0]]
        logger.info(
            "PublishingOrgKeys sin organization en %s: %s",
            integrated,
            f"{len(missing_publisher_keys):,}",
        )

        pub_fetched = 0
        pub_upserted = 0
        pub_errors = 0
        if missing_publisher_keys:
            max_workers = min(20, max(4, len(missing_publisher_keys)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        _fetch_gbif_json,
                        f'https://api.gbif.org/v1/organization/{key}',
                        key,
                        'publishingorgkey',
                    ): key
                    for key in missing_publisher_keys
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        data, ok = future.result()
                    except Exception as e:
                        logger.warning("Error ejecutando tarea GBIF para publishingorgkey %s: %s", key, e)
                        pub_errors += 1
                        continue

                    if not ok:
                        pub_errors += 1
                        continue

                    pub_fetched += 1
                    pub_key = data.get('key') or key
                    logger.info("Ejecutando consulta")
                    conn.execute(publisher_upsert_sql, {
                        'publishingorgkey': pub_key,
                        'organization': data.get('title'),
                        'institutionid': f'https://www.gbif.org/publisher/{pub_key}',
                    })
                    conn.commit()
                    pub_upserted += 1
                    time.sleep(0.002)

        # Integridad referencial: FK sobre claves TEXT del catálogo (NOT VALID = no escanea huérfanos).
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{fk_dataset}"'
        )
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD CONSTRAINT "{fk_dataset}" '
            f'FOREIGN KEY ("datasetkey") '
            f'REFERENCES "gbif_datasets" ("datasetkey") '
            f'ON UPDATE CASCADE '
            f'ON DELETE NO ACTION '
            f'NOT VALID'
        )
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" DROP CONSTRAINT IF EXISTS "{fk_publisher}"'
        )
        conn.commit()
        logger.info("Ejecutando consulta")
        conn.execute(
            f'ALTER TABLE "{integrated}" '
            f'ADD CONSTRAINT "{fk_publisher}" '
            f'FOREIGN KEY ("publishingorgkey") '
            f'REFERENCES "gbif_publishers" ("publishingorgkey") '
            f'ON UPDATE CASCADE '
            f'ON DELETE NO ACTION '
            f'NOT VALID'
        )
        conn.commit()
        logger.info(
            "FK NOT VALID añadidas en %s: %s, %s (VALIDATE CONSTRAINT cuando no queden huérfanos)",
            integrated,
            fk_dataset,
            fk_publisher,
        )

        conn.commit()

    logger.info(
        "Enriquecimiento GBIF datasets en %s (faltantes=%s, consultados=%s, upserts=%s, errores=%s)",
        integrated,
        f"{len(missing_dataset_keys):,}",
        f"{ds_fetched:,}",
        f"{ds_upserted:,}",
        f"{ds_errors:,}",
    )
    logger.info(
        "Enriquecimiento GBIF publishers en %s (faltantes=%s, consultados=%s, upserts=%s, errores=%s)",
        integrated,
        f"{len(missing_publisher_keys):,}",
        f"{pub_fetched:,}",
        f"{pub_upserted:,}",
        f"{pub_errors:,}",
    )
