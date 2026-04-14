# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-03-04
"""
Este archivo contiene las funciones para la carga de datos desde GBIF a un servidor PostgreSQL + PostGIS
para el proceso de análisis y síntesis de cifras para Biodiversidad en cifras.
"""
import csv
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

logger = logging.getLogger('sintesis_biocifras')

# Indica el número de filas que se van a cargar a la base de datos desde los archivos TSV de GBIF 
# en cada batch para evitar bloqueos de memoria.

FLUSH_EVERY = 500_000

_EPOCH_MS_COLS = {'lastinterpreted', 'lastparsed'}


def _epoch_ms_to_iso(value):
    """Convierte epoch en milisegundos a ISO 8601 para columnas TIMESTAMPTZ."""
    if not value:
        return value
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return value

# ------------------------------------------------------------------------------------------------------------
# Definición de listas y variables para el proceso de carga desde los archivos TSV de GBIF
# 
# Para el process de carga desde los archivos occurrence.txt y verbatim.txt se definen únicamente las columnas 
# con listas que se van a utilizar para evitar cargar datos innecesarios y optimizar el proceso de carga.
# Se pueden agregar más columnas si es necesario. Pero no olvidar agregar las columnas a las tablas de staging
# en las listas _OCCURRENCE_TYPES y _VERBATIM_TYPES.
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
    'gbifid': 'BIGINT PRIMARY KEY',
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
    'gbifid': 'BIGINT PRIMARY KEY',
    'type': 'TEXT', 'datasetid': 'TEXT', 'datasetname': 'TEXT',
    'organismquantity': 'TEXT', 'organismquantitytype': 'TEXT',
    'eventid': 'TEXT', 'samplingprotocol': 'TEXT',
    'county': 'TEXT', 'municipality': 'TEXT',
    'repatriated': 'TEXT', 'publishingcountry': 'TEXT',
    'lastparsed': 'TIMESTAMPTZ',
}

_SQL_COL_TYPES = {
    'gbifid': 'BIGINT PRIMARY KEY',
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

# Se crea el motor de conexión a la base de datos PostgreSQL usando SQLAlchemy para crear el pool de conexiones
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


# ----------------------------------------------------------------------------------
# Creación y llenado de la tabla de registro de versiones de tablas (table_registry)
# ----------------------------------------------------------------------------------

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
# Creacion / truncado de tablas de staging (dwc_occurrence y dwc_verbatim) y la tabla integrada (dwc_occurrence_integrated)
# -------------------------------------------------------------------------------------------------------------------------

def _build_create_ddl(table_name, col_types):
    # Genera sentencias CREATE TABLE a partir del diccionario columna -> tipo SQL.
    # Es equivalente a ejecutar la siguiente consulta:
    # CREATE TABLE "tabla_fecha" ("columna1" tipo1, "columna2" tipo2, ...);
    cols = ', '.join(f'"{col}" {dtype}' for col, dtype in col_types.items())
    return f'CREATE TABLE "{table_name}" ({cols});'

# Para mantener un historial de las tablas de staging y la tabla integrada se utiliza un sufijo de fecha.
def tables_operations(engine, suffix, upload_type="default"):
    """Crea tablas con sufijo de fecha. Si ya existen, las elimina y recrea para garantizar un schema limpio. Retorna dict con nombres."""
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
# Carga masiva de datos desde los archivos TSV de GBIF a las tablas de staging (dwc_occurrence y dwc_verbatim)
# ------------------------------------------------------------------------------------------------------------

# Los datos de GBIF pueden presentar problemas por el uso de caracteres especiales como comillas, tabuladores y
# backlash. Por lo que antes de subir cada batch datos se deben procesar los caracteres especiales para evitar
# errores de carga a traves de csv.writer para manejar el caracter comilla doble ("). Para backslash se indica
# en el la sentencia COPY de PostgreSQL con el delimitador E'\\'.

# El otro punto importante es que al cargar los datos se utiliza copy_expert de psycopg2 ya que es más eficiente
# al poder definir cargas por batch y no tener que leer todo el archivo en memoria.
# Ahora, por qué se utiliza copy_expert y no copy_from o directamente a través de SQLAlchemy.execute o una 
# consulta SQL en raw o con el comando COPY de PostgreSQL?
# La principal son los caracteres especiales desde los archivos de GBIF, además de tener control sobre la cantidad
# de filas a cargar por batch.
# COPY si bien es más rápido, hay que procesar los caracteres especiales antes de la carga, pero sobre todo los
# archivos deben estár en el mismo servidor de la base de datos, aunque puede solventarse con salida STDOUT.
# SQLAlchemy.execute es más flexible, pero espera siempre que se retorne el resultado de la consulta, por lo que
# en procesos de carga masiva no es la mejor opción.
# Por ultimo, el comando de copy_expert de psycopg2 se ejecuta a través de la conexión raw de SQLAlchemy,
# que crea un cursor y se ejecuta el comando de copy_expert con el buffer de datos procesado por csv.writer.

def data_upload(engine, filepath, table_name, columns):
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

    db_cols = [c.lower() for c in columns]
    quoted_cols = ', '.join(f'"{c}"' for c in db_cols)
    copy_sql = (
        f'COPY "{table_name}" ({quoted_cols}) '
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"
    )

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        count = 0
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            reader.fieldnames = [name.lower() for name in reader.fieldnames]
            for row in reader:
                writer.writerow([
                    _epoch_ms_to_iso(row.get(c.lower(), '')) if c.lower() in _EPOCH_MS_COLS
                    else row.get(c.lower(), '')
                    for c in columns
                ])
                count += 1
                if count % FLUSH_EVERY == 0:
                    buffer.seek(0)
                    cur.copy_expert(copy_sql, buffer)
                    raw_conn.commit()
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
        raw_conn.close()


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
# Traducción de valores de taxonrank a español en la tabla integrada
# -----------------------------------------------------------------------------------------------------

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


def translate_taxonrank(engine, table_name):
    """Traduce los valores de la columna taxonrank"""
    """ Equivalente a UPDATE "dwc_integrated_{fecha}}"
            SET "taxonrank" = CASE UPPER("taxonrank")
                WHEN 'SPECIES' THEN 'Especie'
                WHEN 'SUBSPECIES' THEN 'Subespecie'
                WHEN 'GENUS' THEN 'Género'
                WHEN 'FAMILY' THEN 'Familia'
                WHEN 'ORDER' THEN 'Orden'
                WHEN 'CLASS' THEN 'Clase'
                WHEN 'PHYLUM' THEN 'Filo'
                WHEN 'KINGDOM' THEN 'Reino'
                WHEN 'FORM' THEN 'Forma'
                WHEN 'VARIETY' THEN 'Variedad'
                WHEN 'UNRANKED' THEN ''
                ELSE ''
            END"""
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

        pk = inspect(engine).get_pk_constraint(integrated)
        if not pk or not pk.get('constrained_columns'):
            conn.execute(text(
                f'ALTER TABLE "{integrated}" ADD PRIMARY KEY ("gbifid")'
            ))
            logger.info("PK a campo gbifid agregada a %s", integrated)
        else:
            logger.info("PK ya existe en %s, se omite creación", integrated)

        conn.execute(text(
            "SELECT AddGeometryColumn(:table, 'the_geom', 4326, 'POINT', 2)"
        ), {'table': integrated})
        conn.execute(text(
            f'UPDATE "{integrated}" '
            f'SET the_geom = ST_SetSRID(ST_MakePoint("decimallongitude", "decimallatitude"), 4326) '
            f'WHERE "decimallatitude" IS NOT NULL AND "decimallongitude" IS NOT NULL'
        ))
        logger.info("Columna the_geom creada con EPSG 4326 en %s", integrated)

        idx_name = f"idx_{integrated}_the_geom"
        conn.execute(text(
            f'CREATE INDEX "{idx_name}" ON "{integrated}" USING GIST (the_geom)'
        ))
        logger.info("Indice GIST creado: %s", idx_name)

        conn.commit()
