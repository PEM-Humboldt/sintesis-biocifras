# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-03-04
"""
Este archivo contiene la configuración del logging para registrar el progreso y errores en pantalla y en archivo de log.
"""

import logging
import time

# Configura logger dual: consola (stdout) + archivo (.log definido en .env).
def setup_logger(log_file_path):
    logger = logging.getLogger('sintesis_biocifras')
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# La función timer mide el tiempo de ejecución de la función que se le pasa como argumento en minutis, así como el texto de logging
def timer(func, label):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger('sintesis_biocifras')
        logger.info("[INICIO] %s", label)
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = (time.time() - start) / 60
        logger.info("[FIN]    %s — %.2f minutos", label, elapsed)
        return result
    return wrapper
