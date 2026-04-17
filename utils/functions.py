# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-03-04
"""
Este archivo contiene las funciones para la carga de datos desde GBIF a un servidor PostgreSQL + PostGIS
para el proceso de análisis y síntesis de cifras para Biodiversidad en cifras.
"""
import csv
import io
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Libreria para la conexión a la base de datos PostgreSQL y PostGIS
from sqlalchemy import create_engine, inspect, text

# Inicialización del logger
logger = logging.getLogger('sintesis_biocifras')

# Indica el número de filas que se van a cargar a la base de datos desde los archivos TSV de GBIF 
# en cada batch para evitar bloqueos de memoria. 
FLUSH_EVERY = 500_000

# ------------------------------------------------------------------------------------------------------------
# Definición de listas y variables para el proceso de carga desde los archivos TSV de GBIF
# 
# Para el process de carga desde los archivos occurrence.txt, verbatim.txt y sql.csv se definen únicamente las columnas 
# con listas que se van a utilizar para evitar cargar datos innecesarios y optimizar el proceso de carga.
# Se pueden agregar más columnas si es necesario. Pero no olvidar agregar las columnas a las tablas de staging
# en las listas _OCCURRENCE_TYPES, _VERBATIM_TYPES, _SQL_COL_TYPES.
# Se decide usar este enfoque de listas para poder agregar o reducir el número de columnas de manera dinámica
# sin tener que modificar directamente consultas SQL en RAW o a través de SQLAlchemy.
# ------------------------------------------------------------------------------------------------------------

OCCURRENCE_COLS = [
    'gbifid', 'occurrenceid', 'basisofrecord', 'collectioncode',
    'catalognumber', 'recordedby', 'individualcount', 'eventdate',
    'countrycode', 'stateprovince', 'locality', 'elevation', 'depth',
    'decimallatitude', 'decimallongitude', 'coordinateuncertaintyinmeters',
    'scientificname', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'infraspecificepithet', 'taxonrank', 'day', 'month',
    'year', 'verbatimscientificname', 'datasetkey', 'publishingorgkey',
    'taxonkey', 'issue', 'occurrencestatus', 'lastinterpreted',
]

VERBATIM_COLS = [
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
    'occurrencestatus', 'lastinterpreted', 'v_type', 'v_datasetid', 'v_datasetname',
    'v_organismquantity', 'v_organismquantitytype', 'v_eventid', 'v_samplingprotocol',
    'v_county', 'v_municipality', 'repatriated', 'publishingcountry', 'lastparsed',
]

# Mapeo de columnas tipo SQL para CREATE TABLE dinamico
_OCCURRENCE_TYPES = {
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

_VERBATIM_TYPES = {
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
    'v_type': 'TEXT', 'v_datasetid': 'TEXT', 'v_datasetname': 'TEXT',
    'v_organismquantity': 'TEXT', 'v_organismquantitytype': 'TEXT',
    'v_eventid': 'TEXT', 'v_samplingprotocol': 'TEXT',
    'v_county': 'TEXT', 'v_municipality': 'TEXT',
    'repatriated': 'BOOLEAN', 'publishingcountry': 'TEXT',
    'lastinterpreted': 'TIMESTAMPTZ', 'lastparsed': 'TIMESTAMPTZ',
}

# ------------------------------------------------------------------------------
# Funciones para la conexión y chequeo de conexión a la base de datos PostgreSQL
# ------------------------------------------------------------------------------

# Se crea el motor de conexión a la base de datos PostgreSQL usando SQLAlchemy para inicializar el pool de conexiones
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASS')}"
        f"@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
    )
    return create_engine(url)

# Comprueba la conexión a la base de datos PostgreSQL a través de la ejecución de una consulta y la verificación de privilegio de creación de base de datos.
def check_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.execute(text(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
            ))
        logger.info("Conexion exitosa a %s@%s:%s/%s",
                     os.getenv('DATABASE_USER'), os.getenv('DATABASE_HOST'),
                     os.getenv('DATABASE_PORT'), os.getenv('DATABASE_NAME'))
        return True
    except Exception as e:
        logger.error("Fallo de conexion: %s", e)
        return False


# -------------------------------------------------------------------------------------------------------
# Creación de tablas de soporte y llenado de la tabla de registro de versiones de tablas (table_registry)
# -------------------------------------------------------------------------------------------------------

def registry_table(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS table_registry (
        id SERIAL PRIMARY KEY,
        table_name TEXT NOT NULL,
        origin TEXT,
        created_at DATE NOT NULL,
        is_latest BOOLEAN NOT NULL DEFAULT TRUE
    );
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    logger.info("Tabla table_registry creada")


def datasets_table(engine):
    """Crea la tabla gbif_datasets para almacenar metadatos de datasets obtenidos desde la API de GBIF."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gbif_datasets (
                datasetkey TEXT PRIMARY KEY,
                license TEXT,
                doi TEXT,
                datasettitle TEXT,
                logourl TEXT,
                datatype TEXT
            );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_gbif_datasets_datasetkey
                ON gbif_datasets USING BTREE (datasetkey);
        """))
        conn.commit()
    logger.info("Tabla gbif_datasets creada")


def publishers_table(engine):
    """Crea la tabla gbif_publishers para almacenar metadatos de publicadores obtenidos desde la API de GBIF."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gbif_publishers (
                publishingorgkey TEXT PRIMARY KEY,
                organization TEXT
            );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_gbif_publishers_publishingorgkey
                ON gbif_publishers USING BTREE (publishingorgkey);
        """))
        conn.commit()
    logger.info("Tabla gbif_publishers creada")

# Con la tabla table_registry se maneja el campo is_latest para indicar
# la versión más reciente de las tablas de staging y la tabla integrada.
def register_load(engine, table_names, created_at, origin):
    prefixes = {
        'occurrence': 'dwc_occurrence_%',
        'verbatim': 'dwc_verbatim_%',
        'integrated': 'dwc_integrated_%',
    }
    with engine.connect() as conn:
        for key, table_name in table_names.items():
            prefix = prefixes[key]
            conn.execute(text(
                "UPDATE table_registry SET is_latest = FALSE "
                "WHERE table_name LIKE :prefix AND is_latest = TRUE"
            ), {'prefix': prefix})
            conn.execute(text(
                "INSERT INTO table_registry (table_name, origin, created_at, is_latest) "
                "VALUES (:table_name, :origin, :created_at, TRUE)"
            ), {'table_name': table_name, 'origin': origin, 'created_at': created_at})
        conn.commit()
    logger.info("Datos cargados en table_registry.")


# -------------------------------------------------------------------------------------------------------------------------
# Creacion / truncado de tablas de staging (dwc_occurrence y dwc_verbatim) y la tabla integrada (dwc_integrated)
# -------------------------------------------------------------------------------------------------------------------------

def _build_create_ddl(table_name, col_types):
    # Función de apoyo.
    # Genera sentencias CREATE TABLE a partir del diccionario columna -> tipo SQL.
    # cols es un diccionario con el nombre de la columna y el tipo SQL que se genera dinámicamente
    # col_types es uno de los diccionarios: _OCCURRENCE_TYPES, _VERBATIM_TYPES, _SQL_COL_TYPES
    # Es equivalente a ejecutar la siguiente consulta:
    # CREATE TABLE "tabla_fecha" ("columna1" tipo1, "columna2" tipo2, ...);
    cols = ', '.join(f'"{col}" {dtype}' for col, dtype in col_types.items())
    return f'CREATE UNLOGGED TABLE "{table_name}" ({cols});'

# Para mantener un historial de las tablas de staging y la tabla integrada se utiliza un sufijo de fecha.
def tables_operations(engine, suffix, upload_type="default"):
    """Crea tablas con sufijo de fecha. Si ya existen, las elimina y vuelven a crear para garantizar una carga limpia. 
    Retorna dict con nombres de las tablas para seguir el proceso de carga.
    Se tienen el cuenta el tipo de carga: sql o regular.
    """
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
            'occurrence': _OCCURRENCE_TYPES,
            'verbatim': _VERBATIM_TYPES,
        }
        keys = ('occurrence', 'verbatim')

    insp = inspect(engine)
    with engine.connect() as conn:
        for key in keys:
            tname = table_names[key]
            if insp.has_table(tname):
                conn.execute(text(f'DROP TABLE "{tname}"'))
                logger.info("DROP TABLE %s", tname)
            ddl = _build_create_ddl(tname, type_maps[key])
            conn.execute(text(ddl))
            logger.info("CREATE TABLE %s", tname)
        conn.commit()
    return table_names


# ------------------------------------------------------------------------------------------------------------
# Carga masiva de datos desde los archivos TSV de GBIF a las tablas de staging
# ------------------------------------------------------------------------------------------------------------

# Los datos de GBIF pueden presentar problemas por el uso de caracteres especiales como comillas, tabuladores y
# backlash. Por lo que antes de subir cada batch datos se deben procesar los caracteres especiales para evitar
# errores de carga a traves de csv.writer para manejar el caracter comilla doble ("). Para backslash se indica
# en el la sentencia COPY de PostgreSQL con el delimitador E'\\'.

# El otro punto importante es que al cargar los datos se utiliza copy_expert de psycopg2 ya que es más eficiente
# al poder hacer cargas por batch y no tener que leer todo el archivo en memoria.
# Ahora, por qué se utiliza copy_expert y no copy_from o directamente a través de SQLAlchemy.execute o una 
# consulta SQL en raw o con el comando COPY de PostgreSQL?
# La principal son los caracteres especiales desde los archivos de GBIF, además de tener control sobre la cantidad
# de filas a cargar por batch.
# COPY si bien es más rápido, hay que procesar los caracteres especiales antes de la carga, pero sobre todo los
# archivos deben estár en el mismo servidor de la base de datos, aunque puede solventarse con salida STDOUT.
# SQLAlchemy.execute es más flexible, pero espera siempre que se retorne el resultado de la consulta, por lo que
# en procesos de carga masiva no es la mejor opción.
# Por último, el comando de copy_expert de psycopg2 se ejecuta a través de la conexión raw de SQLAlchemy,
# que crea un cursor y se ejecuta el comando de copy_expert con el buffer de datos procesado por csv.writer.

# Columnas cargadas desde los archivos TSV de GBIF que se deben convertir a ISO 8601 para columnas TIMESTAMPTZ
# Bug que apareció al cargar los datos descargados en formato GBIF SQL
# El EPOCH es el número de milisegundos desde el 1 de enero de 1970 00:00:00 UTC, pero no es legible como el timestamp
# por lo que se debe convertir a ISO 8601 para que sea legible y que se pueda cargar a la base de datos.
_EPOCH_MS_COLS = {'lastinterpreted', 'lastparsed'}

# Función de apoyo para convertir epoch en milisegundos a ISO 8601 para columnas TIMESTAMPTZ.
def _epoch_ms_to_iso(value):
    """Convierte epoch en milisegundos a ISO 8601 para columnas TIMESTAMPTZ."""
    if not value:
        return value
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return value

def data_upload(engine, filepath, table_name, columns):
    # Confirma que los archivos de datos definidos en el .env existen.
    # Si no existen, se retorna un error y se elimina la tabla de staging.
    if not filepath or not Path(filepath).is_file():
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.commit()
        logger.info("DROP TABLE %s (sin archivo de datos para cargar)", table_name)
        msg = (
            f"No se definió la ruta del archivo en el .env para la tabla {table_name}"
            if not filepath
            else f"El archivo no existe en la ruta indicada en el .env: {filepath}"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    # Se generan las columnas de la tabla de staging en minúsculas para la ejecución del comando COPY de PostgreSQL.
    # Primero se generan las columnas en minúsculas y luego se generan las columnas entre comillas dobles para la ejecución del comando COPY de PostgreSQL.
    # Se ejecuta el comando COPY de PostgreSQL con el formato csv, el delimitador E'\\t' y el null '' para evitar errores de carga.
    db_cols = [c.lower() for c in columns]
    quoted_cols = ', '.join(f'"{c}"' for c in db_cols)
    copy_sql = (
        f'COPY "{table_name}" ({quoted_cols}) '
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"
    )

    # Se crea la conexión raw de SQLAlchemy para ejecutar el comando COPY de PostgreSQL usando psycopg2.
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute("SET synchronous_commit = OFF")
        cur.execute("SET maintenance_work_mem = '4GB'")
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        count = 0
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            # Se lee la primera fila de nombres de columnas y se convierte a minúsculas para la ejecución del comando COPY de PostgreSQL.
            reader.fieldnames = [name.lower() for name in reader.fieldnames]
            for row in reader:
                # Para cada fila, escribe en el buffer solo las columnas que necesita (las definidas en reader.fieldnames)
                # Si la columna es de tipo timestamp (definida en _EPOCH_MS_COLS), convierte el epoch a ISO 8601.
                writer.writerow([
                    _epoch_ms_to_iso(row.get(c.lower(), '')) if c.lower() in _EPOCH_MS_COLS
                    else row.get(c.lower(), '')
                    for c in columns
                ])
                count += 1
                # Si el modulo de count con la variable FLUSH_EVERY se igual a 0 se envía el buffer a la base de datos
                if count % FLUSH_EVERY == 0:
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
        cur.execute("RESET synchronous_commit")
        cur.execute("RESET maintenance_work_mem")
        raw_conn.close()


# -----------------------------------------------------------------------------------------------------
# Renombrado de columnas en la tabla de staging dwc_sql
# -----------------------------------------------------------------------------------------------------

# Se renombran las columnas con prefijo v_ quitando el prefijo y en el caso de scientificname se cambia a verbatimscientificname.
# Se realiza esta función para asegurar que el nombre de las columnas sean consistentes sin importar el origen de los datos.
def rename_sql_columns(engine, table_name, columns):
    """Renombra columnas con prefijo v_ quitando el prefijo (v_scientificname → verbatimscientificname)."""
    renames = {}
    for c in columns:
        if c == 'v_scientificname':
            renames[c] = 'verbatimscientificname'
        elif c.startswith('v_'):
            renames[c] = c[2:]

    with engine.connect() as conn:
        for old, new in renames.items():
            conn.execute(text(
                f'ALTER TABLE "{table_name}" RENAME COLUMN "{old}" TO "{new}"'
            ))
            logger.info("Columna renombrada: %s → %s en %s", old, new, table_name)
        conn.commit()

# Se renombra la tabla de staging dwc_sql a dwc_integrated para integridad en el flujo de carga.
def rename_table(engine, old_name, new_name):
    """Renombra una tabla en la base de datos. Si new_name ya existe, la elimina primero."""
    insp = inspect(engine)
    with engine.connect() as conn:
        if insp.has_table(new_name):
            conn.execute(text(f'DROP TABLE "{new_name}"'))
            logger.info("DROP TABLE existente: %s", new_name)
        conn.execute(text(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"'))
        conn.commit()
    logger.info("Tabla renombrada: %s → %s", old_name, new_name)


# -----------------------------------------------------------------------------------------------------
# Creación de índices en las tablas de staging dwc_occurrence y dwc_verbatim
# -----------------------------------------------------------------------------------------------------

# Se crea un índice en la columna gbifID para facilitar el JOIN entre las tablas de staging.
# Es equivalente a ejecutar la siguiente consulta:
# CREATE INDEX idx_tabla_fecha_gbifid ON tabla_fecha (gbifID);
# Sólo se crea el indice. Para las coordenadas no se crean indices en los staging ya que no se pueden
# copiar directamente a la tabla integrada.

def create_staging_indexes(engine, table_names):
    with engine.connect() as conn:
        for key in ('occurrence', 'verbatim'):
            tname = table_names[key]
            idx_name = f"idx_{tname}_gbifid"
            conn.execute(text(f'CREATE INDEX "{idx_name}" ON "{tname}" ("gbifid")'))
            logger.info("Indice creado: %s", idx_name)
        conn.commit()


# -----------------------------------------------------------------------------------------------------
# Creación de la tabla integrada dwc_occurrence_integrated desde las tablas de staging 
# -----------------------------------------------------------------------------------------------------

# Se crea la tabla integrada dwc_occurrence_integrated desde las tablas de staging dwc_occurrence y dwc_verbatim
# mediante un JOIN por la columna gbifID.
# Es equivalente a ejecutar la siguiente consulta:
# CREATE TABLE dwc_occurrence_integrated AS
# SELECT o.*, v.*
# FROM dwc_occurrence_fecha o
# INNER JOIN dwc_verbatim_fecha v ON o.gbifID = v.gbifID;

def create_integrated_table(engine, table_names):
    occurrence = table_names['occurrence']
    verbatim = table_names['verbatim']
    integrated = table_names['integrated']

    occurrence_cols = ', '.join(
        f'o."{c.lower()}"' for c in OCCURRENCE_COLS
    )
    verbatim_cols = ', '.join(
        f'v."{c.lower()}"' for c in VERBATIM_COLS if c != 'gbifid'
    )
    insp = inspect(engine)
    with engine.connect() as conn:
        if insp.has_table(integrated):
            conn.execute(text(f'DROP TABLE "{integrated}"'))
            logger.info("DROP TABLE existente: %s", integrated)

        sql = (
            f'CREATE TABLE "{integrated}" AS '
            f'SELECT {occurrence_cols}, {verbatim_cols} '
            f'FROM "{occurrence}" o '
            f'INNER JOIN "{verbatim}" v ON o."gbifid" = v."gbifid"'
        )
        conn.execute(text(sql))
        conn.commit()
    logger.info("Tabla integrada creada: %s", integrated)


# -----------------------------------------------------------------------------------------------------
# Preparación y traducción de valores de taxonrank a español en la tabla integrada
# -----------------------------------------------------------------------------------------------------

# Mapeo de valores de taxonrank a español.
_TAXONRANK_MAP = {
    'SPECIES': 'Especie',
    'SUBSPECIES': 'Subespecie',
    'GENUS': 'Género',
    'FAMILY': 'Familia',
    'ORDER': 'Orden',
    'CLASS': 'Clase',
    'PHYLUM': 'Filo',
    'KINGDOM': 'Reino',
    'FORM': 'Forma',
    'VARIETY': 'Variedad',
    'UNRANKED': '',
}

# Se llena el campo species con las dos primeras palabras de scientificname cuando taxonrank es 'SPECIES' y species es nulo o vacío.
# Es equivalente a ejecutar la siguiente consulta:
# UPDATE "dwc_integrated_{fecha}}" SET "species" = TRIM(split_part("scientificname", ' ', 1) || ' ' || split_part("scientificname", ' ', 2)) WHERE UPPER("taxonrank") = 'SPECIES' AND ("species" IS NULL OR TRIM("species") = '')
def fill_species_from_scientificname(engine, table_name):
    """Llena el campo species con las dos primeras palabras de scientificname
    cuando taxonrank es 'SPECIES' y species es nulo o vacío."""
    sql = (
        f'UPDATE "{table_name}" '
        f'SET "species" = TRIM(split_part("scientificname", \' \', 1) '
        f"|| ' ' || split_part(\"scientificname\", ' ', 2)) "
        f'WHERE UPPER("taxonrank") = \'SPECIES\' '
        f'AND ("species" IS NULL OR TRIM("species") = \'\')'
    )
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        conn.commit()
    logger.info("Campo species completado desde scientificname en %s (%s filas)", table_name, f"{result.rowcount:,}")

# Se traduce el valor de taxonrank a español según el mapeo en _TAXONRANK_MAP.
# Es equivalente a ejecutar la siguiente consulta:
# UPDATE "dwc_integrated_{fecha}}" SET "taxonrank" = CASE UPPER("taxonrank")
# WHEN 'SPECIES' THEN 'Especie'
# WHEN 'SUBSPECIES' THEN 'Subespecie'
# WHEN 'GENUS' THEN 'Género'
# WHEN 'FAMILY' THEN 'Familia'
# WHEN 'ORDER' THEN 'Orden'
# WHEN 'CLASS' THEN 'Clase'
# WHEN 'PHYLUM' THEN 'Filo'
# WHEN 'KINGDOM' THEN 'Reino'
# WHEN 'FORM' THEN 'Forma'
# WHEN 'VARIETY' THEN 'Variedad'
# WHEN 'UNRANKED' THEN ''
# ELSE ''
# END
def translate_taxonrank(engine, table_name):
    cases = ' '.join(
        f"WHEN 'UNRANKED' THEN ''" if eng == 'UNRANKED'
        else f"WHEN '{eng}' THEN '{esp}'"
        for eng, esp in _TAXONRANK_MAP.items()
    )
    sql = (
        f'UPDATE "{table_name}" '
        f'SET "taxonrank" = CASE UPPER("taxonrank") {cases} '
        f"ELSE '' END"
    )
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        conn.commit()
    logger.info("Taxonrank traducido en %s (%s filas actualizadas)", table_name, f"{result.rowcount:,}")


# -----------------------------------------------------------------------------------------------------
# Creación de indices y geometrías en la tabla integrada
# -----------------------------------------------------------------------------------------------------

# Se hace una verificación de la extensión postgis para evitar errores de carga.
def add_geometry_and_indexes(engine, table_name):
    integrated = table_name
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        logger.info("Chequeo de extension postgis")

        conn.execute(text(
            f'ALTER TABLE "{integrated}" ADD PRIMARY KEY ("gbifid")'
        ))
        logger.info("PK a campo gbifid agregada a %s", integrated)

        idx_species = f"idx_{integrated}_species"
        conn.execute(text(
            f'CREATE INDEX "{idx_species}" ON "{integrated}" USING BTREE ("species")'
        ))
        logger.info("Indice BTREE creado: %s", idx_species)

        # Se agrega la columna geom a la tabla con la proyección EPSG 4326 utilizando la función AddGeometryColumn de
        # PostGIS. El nombre de la columna es geom para asegurar consistencia con el uso de postgis versión >= 2.0
        conn.execute(text(
            "SELECT AddGeometryColumn(:table, 'geom', 4326, 'POINT', 2)"
        ), {'table': integrated})
        # Se actualiza la columna geom con los valores de decimallongitude y decimallatitude para crear una geometría
        # teniendo en cuenta la condición de que las columnas decimallatitude y decimallongitude no sean nulas.
        conn.execute(text(
            f'UPDATE "{integrated}" '
            f'SET geom = ST_SetSRID(ST_MakePoint("decimallongitude", "decimallatitude"), 4326) '
            f'WHERE "decimallatitude" IS NOT NULL AND "decimallongitude" IS NOT NULL'
        ))
        logger.info("Columna geom creada con EPSG 4326 en %s", integrated)
        # Se crea un índice GIST en la columna geom para facilitar las consultas espaciales.
        idx_name = f"idx_{integrated}_geom"
        conn.execute(text(
            f'CREATE INDEX "{idx_name}" ON "{integrated}" USING GIST (geom)'
        ))
        logger.info("Indice GIST creado: %s", idx_name)

        conn.commit()


# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces espaciales con la tabla MGN_ADM_MPIO_2025 (división político-administrativa) e Invemar_maritime_regions (regiones marítimas)
# --------------------------------------------------------------------------------------------------------------------------------------


# Palabras que se deben convertir a minúsculas después de INITCAP en los campos de departamento y municipio
# para estandarización de nombres. Por ejemplo, 'Norte De Santander' a 'Norte de Santander'.
_LOWERCASE_WORDS = (' De ', ' Y ', ' Del ', ' La ')

def spatials_joins(engine, table_name):
    # Cruza la tabla integrada con MGN_ADM_MPIO_2025 y Invemar_maritime_regions usando ST_Intersects
    # y aplica INITCAP a los campos de departamento y municipio para estandarización de nombres.
    integrated = table_name
    with engine.connect() as conn:
        conn.execute(text(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "codemgn" VARCHAR(5), '
            f'ADD COLUMN IF NOT EXISTS "stateprovincemgn" VARCHAR(250), '
            f'ADD COLUMN IF NOT EXISTS "countymgn" VARCHAR(250), '
            f'ADD COLUMN IF NOT EXISTS "maritimeregion" VARCHAR(250)'
        ))
        logger.info("Columnas codemgn, stateprovincemgn, countymgn, maritimeregion agregadas a %s", integrated)

        conn.execute(text(
            f'UPDATE "{integrated}" i '
            f'SET "codemgn" = m."mpio_cdpmp", '
            f'    "stateprovincemgn" = m."dpto_cnmbr", '
            f'    "countymgn" = m."mpio_cnmbr" '
            f'FROM "MGN_ADM_MPIO_2025" m '
            f'WHERE i.geom IS NOT NULL '
            f'AND ST_Intersects(i.geom, m.geom)'
        ))
        logger.info("Cruce espacial con MGN_ADM_MPIO_2025 completado en %s", integrated)

        conn.execute(text(
            f'UPDATE "{integrated}" i '
            f'SET "maritimeregion" = m."DESCRIP" '
            f'FROM "INVEMAR_MARITIME_REGIONS" m '
            f'WHERE i.geom IS NOT NULL '
            f'AND i."countymgn" IS NULL '
            f'AND ST_Intersects(i.geom, m.geom)'
        ))
        logger.info("Cruce espacial con INVEMAR_MARITIME_REGIONS completado en %s", integrated)

        # Se aplica INITCAP a los campos de departamento y municipio para estandarización de nombres.
        for col in ('stateprovincemgn', 'countymgn'):
            expr = f'INITCAP("{col}")'
            # Se reemplazan las palabras que se deben convertir a minúsculas después de INITCAP en los campos de departamento y municipio
            # Cada palabra en _LOWERCASE_WORDS se formatea para que sea un replace en SQL.
            for word in _LOWERCASE_WORDS:
                expr = f"REPLACE({expr}, '{word}', '{word.lower()}')"
            conn.execute(text(
                f'UPDATE "{integrated}" SET "{col}" = {expr} '
                f'WHERE "{col}" IS NOT NULL'
            ))
        logger.info("INITCAP con estandarizaciones de nombres aplicado a stateprovincemgn y countymgn en %s", integrated)

        conn.commit()

# --------------------------------------------------------------------------------------------------------------------------------------
# Validaciones geográficas
# --------------------------------------------------------------------------------------------------------------------------------------

# Se valida el estado y el municipio contra los valores del cruce con capas MGN y Zonas marítimas
def validate_geography(engine, table_name):
    integrated = table_name

    val_case = (
        "CASE "
        "WHEN UPPER(TRIM(\"{orig}\")) = UPPER(TRIM(\"{mgn}\")) THEN TRUE "
        "WHEN \"{orig}\" IS NULL OR TRIM(\"{orig}\") = '' THEN NULL "
        "WHEN \"decimallatitude\" IS NULL AND \"decimallongitude\" IS NULL THEN NULL "
        "WHEN \"maritimeregion\" IS NOT NULL THEN NULL "
        "ELSE FALSE END"
    )

    validations = {
        'stateprovincevalidation': ('stateprovince', 'stateprovincemgn'),
        'countyvalidation': ('county', 'countymgn'),
    }

    sp_case = val_case.format(orig='stateprovince', mgn='stateprovincemgn')
    co_case = val_case.format(orig='county', mgn='countymgn')

    with engine.connect() as conn:
        cols_ddl = ', '.join(
            f'ADD COLUMN IF NOT EXISTS "{col}" BOOLEAN' for col in validations
        )
        conn.execute(text(
            f'ALTER TABLE "{integrated}" {cols_ddl}, '
            f'ADD COLUMN IF NOT EXISTS "flaggeo" VARCHAR(255)'
        ))

        # Una sola pasada: calcula las validaciones booleanas y flaggeo simultáneamente.
        # flaggeo se deriva inline de las mismas expresiones CASE para evitar depender
        # de columnas que aún no tienen valor en esta misma sentencia.
        conn.execute(text(
            f'UPDATE "{integrated}" SET '
            f'"stateprovincevalidation" = {sp_case}, '
            f'"countyvalidation" = {co_case}, '
            f'"flaggeo" = CASE '
            f'WHEN ({sp_case}) IS TRUE  AND ({co_case}) IS TRUE  THEN NULL '
            f'WHEN ({sp_case}) IS FALSE AND ({co_case}) IS FALSE '
            f"THEN 'Departamento y municipio no coinciden con ubicación de la coordenada' "
            f'WHEN ({sp_case}) IS TRUE  AND ({co_case}) IS FALSE '
            f"THEN 'Municipio no coincide con ubicación de la coordenada' "
            f'WHEN ({sp_case}) IS FALSE AND ({co_case}) IS TRUE '
            f"THEN 'Departamento no coincide con ubicación de la coordenada' "
            f'WHEN ({sp_case}) IS NULL  AND ({co_case}) IS NULL '
            f'AND "maritimeregion" IS NOT NULL '
            f"THEN 'Coordenada en área marítima' "
            f'WHEN ({sp_case}) IS NULL  AND ({co_case}) IS NULL '
            f'AND "decimallatitude" IS NULL AND "decimallongitude" IS NULL '
            f"THEN 'Sin coordenadas' "
            f'ELSE NULL END'
        ))
        logger.info("Validación geográfica completada en %s", integrated)

        conn.commit()


# --------------------------------------------------------------------------------------------------------------------------------------
# Cruces taxonómicos con listados de referencia
# --------------------------------------------------------------------------------------------------------------------------------------

# Se definen las tablas y los campos a cruzar. La idea es iterar sobre las tablas y campos para evitar
# tener que definirlas las consultas SQL manualmente.
# Es equivalente a ejecutar la siguiente consulta:
# UPDATE "dwc_integrated_{fecha}}" SET "cites" = t."cites" FROM "taxonomic_cites" t WHERE i."species" = t."species"
# UPDATE "dwc_integrated_{fecha}}" SET "threatstatusuicn" = t."threatstatusuicn" FROM "taxonomic_threat_uicn" t WHERE i."species" = t."species"
# UPDATE "dwc_integrated_{fecha}}" SET "threatstatusmads" = t."threatstatusmads" FROM "taxonomic_threat_mads" t WHERE i."species" = t."species"
# UPDATE "dwc_integrated_{fecha}}" SET "exotic" = t."exotic", "exoticriskinvasion" = t."exoticriskinvasion", "invasiveness" = t."invasiveness", "invasive" = t."invasive", "transplanted" = t."transplanted" FROM "taxonomic_invasive_exotic" t WHERE i."species" = t."species"
# UPDATE "dwc_integrated_{fecha}}" SET "migratory" = t."migratory", "endemic" = t."endemic" FROM "taxonomic_col_list" t WHERE i."species" = t."species"
# UPDATE "dwc_integrated_{fecha}}" SET "referencelist" = 'Presente en lista taxonómica: ' || "referencelist" FROM "taxonomic_ref_list" t WHERE i."species" = t."species"
_FLAGTAXO_CLASSES = ('Aves', 'Mammalia', 'Reptilia', 'Squamata', 'Crocodylia', 'Testudines')
_FLAGTAXO_ORDERS = ('Lepidoptera',)

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
    'taxonomic_col_list': {
        'columns': {
            'migratory': 'migratory',
            'endemic': 'endemic',
            'datasetid': 'referencelist',
        },
    },
}


def taxonomic_joins(engine, table_name):
    """Cruza la tabla integrada con tablas taxonómicas por el campo species."""
    integrated = table_name
    with engine.connect() as conn:
        for src_table, config in _TAXONOMIC_JOINS.items():
            col_map = config['columns']

            add_cols = ', '.join(
                f'ADD COLUMN IF NOT EXISTS "{dest}" TEXT' for dest in col_map.values()
            )
            conn.execute(text(f'ALTER TABLE "{integrated}" {add_cols}'))

            set_clause = ', '.join(
                f'"{dest}" = t."{src}"' for src, dest in col_map.items()
            )
            conn.execute(text(
                f'UPDATE "{integrated}" i '
                f'SET {set_clause} '
                f'FROM "{src_table}" t '
                f'WHERE i."species" = t."species"'
            ))
            logger.info("Join con %s completado en %s", src_table, integrated)

        conn.execute(text(
            f'UPDATE "{integrated}" '
            f"SET \"referencelist\" = 'Presente en lista taxonómica: ' || \"referencelist\" "
            f'WHERE "referencelist" IS NOT NULL'
        ))
        logger.info("Campo referencelist actualizado en %s", integrated)

        classes_list = ', '.join(f"'{c}'" for c in _FLAGTAXO_CLASSES)
        orders_list = ', '.join(f"'{o}'" for o in _FLAGTAXO_ORDERS)

        conn.execute(text(
            f'ALTER TABLE "{integrated}" ADD COLUMN IF NOT EXISTS "flagtaxo" VARCHAR(255)'
        ))
        conn.execute(text(
            f'UPDATE "{integrated}" SET "flagtaxo" = CASE '
            f'WHEN "referencelist" IS NULL AND "species" IS NOT NULL '
            f"AND \"transplanted\" = 'Trasplantada' "
            f"THEN 'Ausente en lista taxonómica_Trasplantada' "
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
        ))
        logger.info("Campo flagtaxo completado en %s", integrated)

        conn.commit()


def gbif_api_calls(engine, table_name):
    """Enriquece la tabla integrada con metadatos de dataset desde gbif_datasets y, para faltantes, consulta la API de GBIF."""
    integrated = table_name
    with engine.connect() as conn:
        conn.execute(text(
            f'ALTER TABLE "{integrated}" '
            f'ADD COLUMN IF NOT EXISTS "license" TEXT, '
            f'ADD COLUMN IF NOT EXISTS "doi" TEXT, '
            f'ADD COLUMN IF NOT EXISTS "datasettitle" TEXT, '
            f'ADD COLUMN IF NOT EXISTS "logourl" TEXT, '
            f'ADD COLUMN IF NOT EXISTS "datatype" TEXT'
        ))

        conn.execute(text(
            f'UPDATE "{integrated}" i '
            f'SET "license" = d."license", '
            f'    "doi" = d."doi", '
            f'    "datasettitle" = d."datasettitle", '
            f'    "logourl" = d."logourl", '
            f'    "datatype" = d."datatype" '
            f'FROM "gbif_datasets" d '
            f'WHERE i."datasetkey" = d."datasetkey"'
        ))

        missing_rows = conn.execute(text(
            f'SELECT DISTINCT "datasetkey" '
            f'FROM "{integrated}" '
            f'WHERE "datasetkey" IS NOT NULL AND "datasettitle" IS NULL'
        )).fetchall()
        missing_keys = [row[0] for row in missing_rows if row[0]]
        logger.info("Datasetkeys sin datasettitle en %s: %s", integrated, f"{len(missing_keys):,}")

        fetched = 0
        upserted = 0
        errors = 0
        for datasetkey in missing_keys:
            try:
                url = f'http://api.gbif.org/v1/dataset/{datasetkey}'
                with urllib.request.urlopen(url, timeout=10) as response:
                    if response.status != 200:
                        logger.warning("GBIF API status %s para datasetkey %s", response.status, datasetkey)
                        errors += 1
                        continue
                    data = json.loads(response.read().decode('utf-8'))
                fetched += 1

                conn.execute(text("""
                    INSERT INTO gbif_datasets (datasetkey, license, doi, datasettitle, logourl, datatype)
                    VALUES (:datasetkey, :license, :doi, :datasettitle, :logourl, :datatype)
                    ON CONFLICT (datasetkey) DO UPDATE
                    SET license = EXCLUDED.license,
                        doi = EXCLUDED.doi,
                        datasettitle = EXCLUDED.datasettitle,
                        logourl = EXCLUDED.logourl,
                        datatype = EXCLUDED.datatype
                """), {
                    'datasetkey': data.get('key') or datasetkey,
                    'license': data.get('license'),
                    'doi': data.get('doi'),
                    'datasettitle': data.get('title'),
                    'logourl': data.get('logoUrl'),
                    'datatype': data.get('type'),
                })
                upserted += 1
                time.sleep(0.005)
            except urllib.error.HTTPError as e:
                logger.warning("GBIF API status %s para datasetkey %s", e.code, datasetkey)
                errors += 1
            except urllib.error.URLError as e:
                logger.warning("Error de red consultando GBIF API para datasetkey %s: %s", datasetkey, e.reason)
                errors += 1
            except Exception as e:
                logger.warning("Error consultando GBIF API para datasetkey %s: %s", datasetkey, e)
                errors += 1

        conn.execute(text(
            f'UPDATE "{integrated}" i '
            f'SET "license" = d."license", '
            f'    "doi" = d."doi", '
            f'    "datasettitle" = d."datasettitle", '
            f'    "logourl" = d."logourl", '
            f'    "datatype" = d."datatype" '
            f'FROM "gbif_datasets" d '
            f'WHERE i."datasetkey" = d."datasetkey"'
        ))
        conn.commit()

    logger.info(
        "Enriquecimiento GBIF datasets completado en %s (faltantes=%s, consultados=%s, upserts=%s, errores=%s)",
        integrated,
        f"{len(missing_keys):,}",
        f"{fetched:,}",
        f"{upserted:,}",
        f"{errors:,}",
    )
