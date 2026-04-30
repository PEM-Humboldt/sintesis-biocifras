# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-03-04
"""
Script principal para orquestar el proceso de carga de datos desde GBIF
a un servidor PostgreSQL + PostGIS para el proceso de análisis y síntesis
de cifras para Biodiversidad en cifras.

El script sigue los siguientes pasos:
1. Configurar el logging para registrar el progreso y errores en pantalla y en archivo de log(utils/logger.py).
2. Comprobar conexión a la base de datos PostgreSQL (utils/functions.py -> check_connection).
3. Crear/truncar las tablas de staging (dwc_occurrence y dwc_verbatim) y la tabla integrada (dwc_occurrence_integrated) (utils/functions.py -> tables_operations).
4. Cargar los datos desde occurrence.txt y verbatim.txt desde archivos descomprimidos de GBIF (utils/functions.py -> data_upload).
5. Crear índices en las tablas de staging para el campo gbifID (utils/functions.py -> create_staging_indexes).
6. Crear la tabla integrada con las columnas de las tablas de staging (utils/functions.py -> create_integrated_table).
7. Añadir geometría con base a las coordenadas decimales y índices a la tabla integrada (utils/functions.py -> add_geometry_and_indexes).
8. Registrar la carga en la tabla de registro de versiones de tablas (table_registry) (utils/functions.py -> register_load).
9. Registrar el fin del proceso (utils/logger.py -> logger.info).
"""

import os
import sys
# Para tener la fecha actual de ejecución del script y para el sufijo de las tablas de staging y la tabla integrada
from datetime import date
# Leer variables de entorno desde archivo .env en la raíz del proyecto
from dotenv import load_dotenv
# Importar funciones de utils/logger.py y utils/functions.py
from utils.logger import setup_logger, timer
from utils.connection import get_db, check_connection

load_dotenv()

from utils.functions import (
    OCCURRENCE_COLS,
    VERBATIM_COLS,
    SQL_COLS,
    register_load,
    tables_operations,
    data_upload,
    finalize_sql_table,
    create_staging_indexes,
    create_integrated_table,
    fill_species_from_scientificname,
    add_geometry_and_indexes,
    create_join_validation_columns,
    create_species_index,
    spatials_joins,
    normalize_stateprovince_county,
    validate_geography,
    taxonomic_joins,
    clean_threatstatus_fields,
    gbif_api_calls,
)

# Tipo de carga: sql (descarga GBIF desde GBIF API SQL API) o regular (descarga archivo interpretado y DwC-A desde GBIF
UPLOAD_TYPE = "sql"
# Tamaño del buffer para la carga de datos.
FLUSH_EVERY = int(os.getenv('FLUSH_EVERY', '500000'))

logger = setup_logger(os.getenv('LOG_FILE_PATH'))
today = date.today()
#Formato de la fecha para el sufijo de las tablas de staging y la tabla integrada
suffix = today.strftime('%Y%m%d')

logger.info("Inicio del proceso de carga — sufijo: %s", suffix)
# Obtener la conexión a la base de datos PostgreSQL usando psycopg2
db = get_db()

if not check_connection(db):
    logger.error("No se pudo conectar a la base de datos. Verifique los valores de conexión en .env")
    sys.exit(1)

logger.info("Conectado a la base de datos.")

try:
    table_integrated_name = f'dwc_integrated_{suffix}'

    if UPLOAD_TYPE == "sql":
        table_names = timer(tables_operations, "Operaciones sobre la tabla de staging dwc_sql")(
            db, suffix, upload_type=UPLOAD_TYPE
        )
        timer(data_upload, "Carga de datos desde SQL_FILE")(
            db, os.getenv('SQL_FILE'), table_names['sql'], SQL_COLS, FLUSH_EVERY
        )
        timer(finalize_sql_table, "Renombrando campos y tabla SQL a integrated")(
            db, table_names['sql'], table_integrated_name
        )
        table_names = {'integrated': table_integrated_name}
        origin = 'SQL download'
    else:
        table_names = timer(tables_operations, "Operaciones sobre las tablas de staging dwc_occurrence y dwc_verbatim")(db, suffix)
        timer(data_upload, "Carga de datos desde occurrence.txt")(
            db, os.getenv('OCCURRENCE_FILE'), table_names['occurrence'], OCCURRENCE_COLS, FLUSH_EVERY
        )
        timer(data_upload, "Carga de datos desde verbatim.txt")(
            db, os.getenv('VERBATIM_FILE'), table_names['verbatim'], VERBATIM_COLS, FLUSH_EVERY
        )
        timer(create_staging_indexes, "Creación de índices en las tablas de staging dwc_occurrence y dwc_verbatim")(db, table_names)
        timer(create_integrated_table, "Creación de la tabla integrada dwc_occurrence_integrated")(db, table_names)
        origin = 'DwC-A download'

    timer(create_join_validation_columns, "Crando columnas para cruces y validaciones en la tabla integrada")(db, table_names['integrated'])
    timer(fill_species_from_scientificname, "Completando campo species desde scientificname")(db, table_names['integrated'])
    timer(add_geometry_and_indexes, "Añadiendo PK y geometría base a la tabla integrada")(db, table_names['integrated'])
    timer(spatials_joins, "Cruce espacial con MGN departamentos y municipios y zonas marítimas")(db, table_names['integrated'])
    #timer(normalize_stateprovince_county, "Normalizando stateprovince/county antes de validación")(db, table_names['integrated'])
    #timer(validate_geography, "Validación geográfica")(db, table_names['integrated'])
    #timer(create_species_index, "Creando índice BTREE de species en la tabla integrada")(db, table_names['integrated'])
    #timer(taxonomic_joins, "Cruces taxonómicos con listados")(db, table_names['integrated'])
    #timer(clean_threatstatus_fields, "Normalizando campos threatstatus antes de API")(db, table_names['integrated'])
    #timer(gbif_api_calls, "Enriqueciendo metadatos de datasets y publicadores GBIF")(db, table_names['integrated'])

    register_load(db, table_names, today, origin)
    logger.info("Proceso completado.")

# Captura de errores generales del proceso.
except (FileNotFoundError, ValueError) as e:
    logger.error("Error durante el proceso: %s", e)
    sys.exit(1)
except Exception as e:
    logger.error("Error durante el proceso: %s", e, exc_info=True)
    sys.exit(1)

finally:
    db.dispose()
