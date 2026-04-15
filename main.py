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
from utils.functions import (
    OCCURRENCE_COLS,
    VERBATIM_COLS,
    SQL_COLS,
    get_engine,
    check_connection,
    registry_table,
    register_load,
    tables_operations,
    data_upload,
    rename_sql_columns,
    rename_table,
    create_staging_indexes,
    create_integrated_table,
    fill_species_from_scientificname,
    translate_taxonrank,
    add_geometry_and_indexes,
    spatial_join_mgn,
)

UPLOAD_TYPE = "sql"

load_dotenv()

logger = setup_logger(os.getenv('LOG_FILE_PATH'))
today = date.today()
#Formato de la fecha para el sufijo de las tablas de staging y la tabla integrada
suffix = today.strftime('%Y%m%d')

logger.info("Inicio del proceso de carga — sufijo: %s", suffix)
# Obtener el motor de conexión a la base de datos PostgreSQL usando SQLAlchemy para crear el pool de conexiones
engine = get_engine()

if not check_connection(engine):
    logger.error("No se pudo conectar a la base de datos. Verifique los valores de conexión en .env")
    sys.exit(1)

logger.info("Conectado a la base de datos.")

try:
    registry_table(engine)

    integrated_name = f'dwc_integrated_{suffix}'

    if UPLOAD_TYPE == "sql":
        table_names = timer(tables_operations, "Operaciones sobre la tabla de staging dwc_sql")(
            engine, suffix, upload_type=UPLOAD_TYPE
        )
        timer(data_upload, "Carga de datos desde SQL_FILE")(
            engine, os.getenv('SQL_FILE'), table_names['sql'], SQL_COLS
        )
        timer(rename_sql_columns, "Renombrando columnas verbatim en tabla SQL")(
            engine, table_names['sql'], SQL_COLS
        )
        timer(rename_table, "Renombrando tabla SQL a integrated")(
            engine, table_names['sql'], integrated_name
        )
        table_names = {'integrated': integrated_name}
        origin = 'sql download'
    else:
        table_names = timer(tables_operations, "Operaciones sobre las tablas de staging dwc_occurrence y dwc_verbatim")(engine, suffix)
        timer(data_upload, "Carga de datos desde occurrence.txt")(
            engine, os.getenv('OCCURRENCE_FILE'), table_names['occurrence'], OCCURRENCE_COLS
        )
        timer(data_upload, "Carga de datos desde verbatim.txt")(
            engine, os.getenv('VERBATIM_FILE'), table_names['verbatim'], VERBATIM_COLS
        )
        timer(create_staging_indexes, "Creación de índices en las tablas de staging dwc_occurrence y dwc_verbatim")(engine, table_names)
        timer(create_integrated_table, "Creación de la tabla integrada dwc_occurrence_integrated")(engine, table_names)
        origin = 'regular download'

    timer(fill_species_from_scientificname, "Completando campo species desde scientificname")(engine, table_names['integrated'])
    timer(translate_taxonrank, "Traduciendo taxonrank a español")(engine, table_names['integrated'])
    timer(add_geometry_and_indexes, "Añadiendo geometría e índices")(engine, table_names['integrated'])
    timer(spatial_join_mgn, "Cruce espacial con MGN municipios")(engine, table_names['integrated'])

    register_load(engine, table_names, today, origin)
    logger.info("Proceso completado.")

except (FileNotFoundError, ValueError) as e:
    logger.error("Error durante el proceso: %s", e)
    sys.exit(1)
except Exception as e:
    logger.error("Error durante el proceso: %s", e, exc_info=True)
    sys.exit(1)

finally:
    engine.dispose()
