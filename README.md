# Scripts para síntesis de cifras

Scripts de lectura, carga y síntesis de datos desde GBIF hacia PostgreSQL + PostGIS, para el análisis de cifras de Biodiversidad en cifras.

El código se encuentra en modificación permanente.

## Prerequisitos

El proyecto está desarrollado en Python 3.10 y se conecta a una base de datos PostgreSQL 18 con PostGIS. Los paquetes necesarios están en `requirements.txt` (conexión a Postgres, descargas y llamadas a API, y carga de variables desde `.env`).

Hay un archivo `.env-template` con las variables de conexión, rutas de archivos y ajustes de rendimiento. Hay que copiarlo a `.env` y completar los valores.

Se debe restaurar la base de datos en blanco (carpeta `dump`). El nombre por defecto es `sintesis_biocifras`. El dump es un archivo plano y se puede restaurar con:

```bash
psql sintesis_biocifras < dump_sintesis_blankdb.sql
```

También es preferible usar un usuario con privilegios de `SELECT`, `UPDATE`, `INSERT`, `ALTER`, `CREATE` y `DELETE` solo sobre esta base de datos.

### Archivos necesarios

Hay dos formas de obtener los registros de Colombia desde [GBIF](https://www.gbif.org/occurrence/download?country=CO&occurrence_status=present) (ocurrencias presentes) o través del servicio de descarga or SQL de [GBIF](https://www.gbif.org/occurrence/download/sql#about). Se necesita cuenta de usuario en GBIF; la generación del archivo puede tardar y el aviso llega por correo.

1. **Descarga DarwinCore (DwC-A):** Del descargado, se usan el `occurrence.txt` y también se dercarga la versión interpretada (`csv`). 
2. **Descarga por API SQL:** un CSV único definido en `SQL_FILE` del `.env`.

Además, `initialize.py` carga capas geográficas, listas taxonómicas y archivos de apoyo (estimadas por departamento, metadatos de especies, publicadores, etc.). Las rutas se configuran en el `.env` (`FILE_*`, `EXTDATADIR`, etc.).

Para exportar las cifras a archivos, definir `EXPORT_DIR` en el `.env`.

## Cómo ejecutar

Clonar el código desde el repositorio.

Es preferible usar un entorno virtual:

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

(Se puede cambiar `myvenv` por otro nombre.)

Instalar dependencias:

```bash
pip install -r requirements.txt
```

Preparar el entorno:

```bash
cp .env-template .env
vi .env
```

### 1. Inicializar la base

Carga tablas de referencia (geografía, taxonomía, estimadas, publicadores, etc.):

```bash
python3 initialize.py
```

### 2. Cargar y enriquecer los registros

Lee la descarga de GBIF, arma la tabla integrada y aplica validaciones geográficas y taxonómicas:

```bash
python3 main.py
```

### 3. Generar las cifras (vistas materializadas)

Construye las vistas que consume el portal (especies, regiones, temáticas, publicadores, estimadas, etc.):

```bash
python3 stats_generator.py
```

Se pueden omitir bloques concretos con flags `--skip-*` (ver `python3 stats_generator.py --help`).

En ejecución bajo entornos Windows con WSL (Subsistema Linux) conviene dejar `MAX_PARALLEL_WORKERS_MV=0` y un `WORK_MEM` moderado en el `.env`, para evitar problemas de memoria compartida. En un Linux nativo o en Windows sin usas WSL se pueden subir esos valores.

### 4. Exportar a TSV (opcional)

Vuelca las vistas de producto a archivos tabulados en `EXPORT_DIR`:

```bash
python3 exporter.py
```

Genera, entre otros: `publicador`, `region_publicador`, `especie`, `especie_meta`, `especie_grupo`, `especie_region`, `especie_tematica`, `cifras_totales`, `region_tematica` y `region_grupo`.

## Modelo de datos

Un diagrama de las tablas base (sin vistas materializadas ni tablas temporales) está en [`Documentation/er_modelo_entidad_relacion.png`](Documentation/er_modelo_entidad_relacion.png).

## Documentación adicional

En la carpeta `Documentation/` hay notas sobre descargas GBIF, listas de referencia y ejemplos de consulta.

## Problemas conocidos

¡Ver los problemas en issues! No duden en documentar otros problemas potenciales.

## Autores(as) y contacto

* **Diego Moreno** - *PS* - [damorenov](https://github.com/damorenov)
* **Marius Bottin** - supervisor del contrato para el instituto Humboldt, apoyo punctual en la programación - [marbotte](https://github.com/marbotte)

## Licencia

Este proyecto está bajo la licencia MIT, mira la [LICENCIA](LICENSE) para obtener más detalles.
