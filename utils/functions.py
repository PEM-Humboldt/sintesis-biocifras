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
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

logger = logging.getLogger('sintesis_biocifras')

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
    'gbifID', 'occurrenceID', 'basisOfRecord', 'collectionCode',
    'catalogNumber', 'recordedBy', 'individualCount', 'eventDate',
    'countryCode', 'stateProvince', 'locality', 'elevation', 'depth',
    'decimalLatitude', 'decimalLongitude', 'coordinateUncertaintyInMeters',
    'scientificName', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'infraspecificEpithet', 'taxonRank', 'day', 'month',
    'year', 'verbatimScientificName', 'datasetKey', 'publishingOrgKey',
    'taxonKey', 'issue', 'occurrenceStatus', 'lastInterpreted',
]

VERBATIM_COLS = [
    'gbifID', 'type', 'datasetID', 'datasetName', 'organismQuantity',
    'organismQuantityType', 'eventID', 'samplingProtocol', 'county',
    'municipality', 'repatriated', 'publishingCountry', 'lastParsed',
]

# Mapeo de columnas tipo SQL para CREATE TABLE dinamico
_OCCURRENCE_TYPES = {
    'gbifID': 'BIGINT PRIMARY KEY',
    'occurrenceID': 'TEXT', 'basisOfRecord': 'TEXT',
    'collectionCode': 'TEXT', 'catalogNumber': 'TEXT',
    'recordedBy': 'TEXT', 'individualCount': 'INTEGER',
    'eventDate': 'TEXT', 'countryCode': 'TEXT',
    'stateProvince': 'TEXT', 'locality': 'TEXT',
    'elevation': 'DOUBLE PRECISION', 'depth': 'DOUBLE PRECISION',
    'decimalLatitude': 'DOUBLE PRECISION',
    'decimalLongitude': 'DOUBLE PRECISION',
    'coordinateUncertaintyInMeters': 'DOUBLE PRECISION',
    'scientificName': 'TEXT', 'kingdom': 'TEXT', 'phylum': 'TEXT',
    'class': 'TEXT', 'order': 'TEXT', 'family': 'TEXT',
    'genus': 'TEXT', 'species': 'TEXT', 'infraspecificEpithet': 'TEXT',
    'taxonRank': 'TEXT', 'day': 'SMALLINT', 'month': 'SMALLINT',
    'year': 'SMALLINT', 'verbatimScientificName': 'TEXT',
    'datasetKey': 'TEXT', 'publishingOrgKey': 'TEXT',
    'taxonKey': 'BIGINT', 'issue': 'TEXT', 'occurrenceStatus': 'TEXT',
    'lastInterpreted': 'TIMESTAMPTZ',
}

_VERBATIM_TYPES = {
    'gbifID': 'BIGINT PRIMARY KEY',
    'type': 'TEXT', 'datasetID': 'TEXT', 'datasetName': 'TEXT',
    'organismQuantity': 'TEXT', 'organismQuantityType': 'TEXT',
    'eventID': 'TEXT', 'samplingProtocol': 'TEXT',
    'county': 'TEXT', 'municipality': 'TEXT',
    'repatriated': 'TEXT', 'publishingCountry': 'TEXT',
    'lastParsed': 'TIMESTAMPTZ',
}

# Indica el número de filas que se van a cargar en cada batch para evitar bloqueos de memoria.
FLUSH_EVERY = 500_000


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

def register_load(engine, table_names, created_at):
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
                "INSERT INTO table_registry (table_name, created_at, is_latest) "
                "VALUES (:table_name, :created_at, TRUE)"
            ), {'table_name': table_name, 'created_at': created_at})
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
def tables_operations(engine, suffix):
    """Crea o trunca tablas con sufijo de fecha. Retorna dict con nombres."""
    table_names = {
        'occurrence': f'dwc_occurrence_{suffix}',
        'verbatim': f'dwc_verbatim_{suffix}',
        'integrated': f'dwc_integrated_{suffix}',
    }
    type_maps = {
        'occurrence': _OCCURRENCE_TYPES,
        'verbatim': _VERBATIM_TYPES,
    }
    # Se utiliza el inspector de SQLAlchemy para verificar si las tablas existen y para crearlas o truncarlas.
    # Es equivalente a ejecutar la siguiente consulta:
    # SELECT * FROM information_schema.tables WHERE table_name = 'tabla_fecha';
    insp = inspect(engine)
    with engine.connect() as conn:
        for key in ('occurrence', 'verbatim'):
            tname = table_names[key]
            # Si la tablas existen (tabla_fecha), se trunca la información ya que se asume que se cargan nuevos datos al día.
            if insp.has_table(tname):
                conn.execute(text(f'TRUNCATE TABLE "{tname}"'))
                logger.info("TRUNCATE en %s", tname)
            else:
                # Si la tablas no existen (tabla_fecha), se crea la tabla con las columnas definidas en las listas _OCCURRENCE_TYPES y _VERBATIM_TYPES.
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
    quoted_cols = ', '.join(f'"{c}"' for c in columns)
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
        # Se lee el archivo y se procesa linea a linea para el manejo de caracteres especiales
        # Cuando se leen N linea (variable FLUSH_EVERY) se cargan en un buffer y se carga a la base de datos
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                writer.writerow([row.get(c, '') for c in columns])
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
            conn.execute(text(f'CREATE INDEX "{idx_name}" ON "{tname}" ("gbifID")'))
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
        f'o."{c}"' for c in OCCURRENCE_COLS
    )
    verbatim_cols = ', '.join(
        f'v."{c}"' for c in VERBATIM_COLS if c != 'gbifID'
    )
    # Se revisa que la tabla integrada por fecha no exista para evitar errores de duplicación.
    insp = inspect(engine)
    with engine.connect() as conn:
        if insp.has_table(integrated):
            conn.execute(text(f'DROP TABLE "{integrated}"'))
            logger.info("DROP TABLE existente: %s", integrated)

        sql = (
            f'CREATE TABLE "{integrated}" AS '
            f'SELECT {occurrence_cols}, {verbatim_cols} '
            f'FROM "{occurrence}" o '
            f'INNER JOIN "{verbatim}" v ON o."gbifID" = v."gbifID"'
        )
        conn.execute(text(sql))
        conn.commit()
    logger.info("Tabla integrada creada: %s", integrated)


# -----------------------------------------------------------------------------------------------------
# Creación de indices y geometrías en la tabla integrada
# -----------------------------------------------------------------------------------------------------

# Se hace una verificación de la extensión postgis para evitar errores de carga.

def add_geometry_and_indexes(engine, table_names):
    integrated = table_names['integrated']
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        logger.info("Chequeo de extension postgis")

        conn.execute(text(
            f'ALTER TABLE "{integrated}" ADD PRIMARY KEY ("gbifID")'
        ))
        logger.info("PK a campo gbifID agregada a %s", integrated)

        conn.execute(text(
            f'ALTER TABLE "{integrated}" ADD COLUMN geometry GEOMETRY(Point, 4326)'
        ))
        conn.execute(text(
            f'UPDATE "{integrated}" '
            f'SET geometry = ST_SetSRID(ST_MakePoint("decimalLongitude", "decimalLatitude"), 4326) '
            f'WHERE "decimalLatitude" IS NOT NULL AND "decimalLongitude" IS NOT NULL'
        ))
        logger.info("Columna geometry creada con EPSG 4326 en %s", integrated)

        idx_name = f"idx_{integrated}_geometry"
        conn.execute(text(
            f'CREATE INDEX "{idx_name}" ON "{integrated}" USING GIST (geometry)'
        ))
        logger.info("Indice GIST creado: %s", idx_name)

        conn.commit()
