# Scripts para sintesis de cifras

Script de lectura y carga de datos desde GBIF a un servidor PostgreSQL + PostGIS para el proceso de análisis y síntesis de cifras para Biodiversidad en cifras

El script se encuentra en modificación permanente.

## Prerequisitos

El script está desarrollado en Python versión 3.10, el cual se conecta a una base de datos PostgreSQL versión 18. Los requisitos están en el archivo de requirements.txt (psycopg2 conexión a postgres, request para descargas y llamados a API, y python-dotenv para carga de datos desde un archivo de variables de entorno). También se tiene un archivo .env_template para definir las variables de conexión y otras configuraciones

Se debe restaurar la base de datos en blanco (encontrada dentro de la carpeta dump). El nombre por defecto de la base de datos es `sintesis_biocifras`. El dump es un archivo plano por lo que se puede restaurar con el comando `psql sintesis_biocifras < dump_sintesis_blankdb.sql`.
También es preferible utilizar un usuario y contraseña con privilegios de `SELECT`, `UPDATE`, `INSERT`, `ALTER`, `CREATE` y `DELETE` sólo a esta base de datos.

### Archivos Necesarios

Descarga del archivo DarwinCore desde [GBIF](https://www.gbif.org/occurrence/download?country=CO&occurrence_status=present) para Colombia con estado de ocurrencia presente. Se debe contar con una cuenta de usario de GBIF para preparar la consulta. Por el tamaño de la misma la generación puede llevar un tiempo y cuando la descarga esté lista, el sistema de correo de GBIF informará el enlace para descarga de información.

Dentro del archivo descargado se utilizan el interpretado (`occurrence.txt`)y los datos en verbatim (`verbatim.txt`) 

## Como ejecutar

Clonar el código desde el repositorio

Es preferible establecer un virtual environment para ejecutar el script.

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

(Se puede cambiar `myvenv` con otro nombre)

Instalar los requerimientos con PIP

```bash
pip install -r requirements.txt
```

Hacer copia del archivo .env_template y dejarlo como .env

```bash
cp .env_template .env
```

Modificar los parámetros dentro del .env

```bash
vi .env
```

Ejecutar el script

```bash
python3 main.py
```

## Problemas conocidos

Ninguno hasta el momento :P

## Autores(as) y contacto

* **Diego Moreno** - *PS* - [damorenov](https://github.com/damorenov)

## Licencia

Este proyecto está bajo la licencia MIT, mira la [LICENCIA](LICENSE) para obtener más detalles.
