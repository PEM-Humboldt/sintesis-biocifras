# Descargar datos desde pygbif
Marius Bottin

## `pyGbif`: la lista de descargas

`pyGbif` es un paquete de python que permite manejar la API de GBIF. En
particular, el modulo `occurrences` permite manejar las descargas de
conjunto de datos a través de la API. Sin embargo es importante anotar
que el modulo no permite importar los conjuntos de datos, simplemente su
descarga.

Primero es importante incluir el archivo .env con los usuarios y
contraseñas de GBIF.

``` python
import os
from dotenv import load_dotenv, dotenv_values
from pygbif import occurrences as occ
from tabulate import tabulate
import pandas as pd
import json
load_dotenv()
```

    True

``` python
print(os.getenv("GBIF_USER"))
```

    bottinmarius

La lista de descarga permite descargar los metadatos de las descargas
presentes en el perfil del usuario de GBIF

``` python
#ref_occ_down = occ.download_sql("SELECT gbifid, ScientificName, countryCode FROM occurrence WHERE genus='Espeletia' LIMIT 10")
#occ.download_meta(ref_occ_down)
downList = occ.download_list()
df_downList = pd.json_normalize(downList["results"])
df_downList.columns
```

Index(\[‘key’, ‘doi’, ‘license’, ‘created’, ‘modified’, ‘eraseAfter’,
‘status’, ‘downloadLink’, ‘size’, ‘totalRecords’, ‘numberDatasets’,
‘source’, ‘request.sql’, ‘request.creator’,
‘request.notificationAddresses’, ‘request.sendNotification’,
‘request.format’, ‘request.type’, ‘request.predicate.type’,
‘request.predicate.key’, ‘request.predicate.value’,
‘request.predicate.matchCase’, ‘request.verbatimExtensions’,
‘request.interpretedExtensions’\], dtype=‘str’)

``` python
df_downList2 = df_downList[['key', 'doi','created','status','request.sendNotification','downloadLink']]
print(tabulate(df_downList2, headers = 'keys', tablefmt = 'github'))
```

|  | key | doi | created | status | request.sendNotification | downloadLink |
|----|----|----|----|----|----|----|
| 0 | 0002968-260409193756587 | 10.15468/dl.d5u7xz | 2026-04-11T03:02:40.859+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0002968-260409193756587.zip |
| 1 | 0002247-260409193756587 | 10.15468/dl.q749s6 | 2026-04-10T20:25:47.789+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0002247-260409193756587.zip |
| 2 | 0001627-260409193756587 | 10.15468/dl.b8bq55 | 2026-04-10T15:45:40.874+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0001627-260409193756587.zip |
| 3 | 0001625-260409193756587 | 10.15468/dl.b3vtxm | 2026-04-10T15:44:48.512+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0001625-260409193756587.zip |
| 4 | 0001617-260409193756587 | 10.15468/dl.h2hk99 | 2026-04-10T15:40:48.745+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0001617-260409193756587.zip |
| 5 | 0082621-260226173443078 | 10.15468/dl.pywwwt | 2026-04-03T21:01:03.966+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0082621-260226173443078.zip |
| 6 | 0082571-260226173443078 | 10.15468/dl.hf8r23 | 2026-04-03T20:46:34.621+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0082571-260226173443078.zip |
| 7 | 0065380-260226173443078 | 10.15468/dl.y8375s | 2026-03-27T21:06:00.411+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0065380-260226173443078.zip |
| 8 | 0065368-260226173443078 | nan | 2026-03-27T20:48:37.673+00:00 | CANCELLED | False | https://api.gbif.org/v1/occurrence/download/request/0065368-260226173443078.zip |
| 9 | 0064920-260226173443078 | nan | 2026-03-27T15:08:16.378+00:00 | FAILED | False | https://api.gbif.org/v1/occurrence/download/request/0064920-260226173443078.zip |
| 10 | 0064894-260226173443078 | 10.15468/dl.ytr8y9 | 2026-03-27T14:55:36.702+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0064894-260226173443078.zip |
| 11 | 0064859-260226173443078 | 10.15468/dl.2zptkn | 2026-03-27T14:41:33.806+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0064859-260226173443078.zip |
| 12 | 0064834-260226173443078 | 10.15468/dl.k8xwmr | 2026-03-27T14:28:53.677+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0064834-260226173443078.zip |
| 13 | 0027989-180131172636756 | 10.15468/dl.9ggjke | 2018-04-02T14:59:29.957+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0027989-180131172636756.zip |
| 14 | 0027985-180131172636756 | 10.15468/dl.awmqai | 2018-04-02T14:27:14.581+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0027985-180131172636756.zip |
| 15 | 0095338-160910150852091 | 10.15468/dl.reshq8 | 2017-05-30T19:48:22.381+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0095338-160910150852091.zip |
| 16 | 0095337-160910150852091 | 10.15468/dl.xqndaq | 2017-05-30T19:48:10.703+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0095337-160910150852091.zip |
| 17 | 0093362-160910150852091 | 10.15468/dl.kvw28z | 2017-05-23T22:28:22.010+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0093362-160910150852091.zip |
| 18 | 0093356-160910150852091 | 10.15468/dl.goozhi | 2017-05-23T22:17:54.090+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0093356-160910150852091.zip |
| 19 | 0092947-160910150852091 | 10.15468/dl.9vdjro | 2017-05-22T21:52:10.278+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0092947-160910150852091.zip |

Podemos utilizar esta lista para probar si ya se descargo, a través de
la API SQL una consulta SQL (Nota: imagino que cuando son consultas
complejas, podríamos tener problemas con los cambios de linea):

``` python
def is_in_my_download_list(sql_query, limit=20):
  downList = occ.download_list(limit=limit)
  df_downList = pd.json_normalize(downList["results"])
  df_downList_SQLok = df_downList[df_downList["request.sql"] == sql_query]
  tableOK = df_downList_SQLok.query('status == "PREPARING" or status == "RUNNING" or status == "SUCCEEDED"')
  sizeMatch = tableOK.size
  return sizeMatch>0

query = "SELECT gbifid, ScientificName, countryCode FROM occurrence WHERE genus='Espeletia' LIMIT 10"
is_in_my_download_list(query)
```

    True

Incluso podemos mirar cual es el “key” de descarga que corresponde a la
consulta SQL:

``` python
def get_query_key(sql_query, limit=20):
  downList = occ.download_list(limit=limit)
  df_downList = pd.json_normalize(downList["results"])
  df_downList_SQLok = df_downList[df_downList["request.sql"] == sql_query]
  tableOK = df_downList_SQLok.query('status == "PREPARING" or status == "RUNNING" or status == "SUCCEEDED"')
  lastKey = tableOK['key'].values[0]
  return lastKey

get_query_key(query)
```

    '0002247-260409193756587'

## Hacer una función Wait

Gracias a la función `download_meta` podemos mirar el estatus de la
descarga, que cuando todo va bien pasa de ‘PREPARING’ a ‘RUNNING’ a
‘SUCCEEDED’

``` python
def download_status(key):
  status = occ.download_meta(key)['status']
  return status
  
download_status(get_query_key(query))
```

    'SUCCEEDED'

Entonces, podemos crear una función wait, para esperar que el estatus
llegue a ‘SUCCEEDED’, y muestre los cambios de estatus:

``` python
"""
Potential statuses of a download in gbif PREPARING, RUNNING, SUCCEEDED, CANCELLED, KILLED, FAILED, SUSPENDED, FILE_ERASED
"""
```

    '\nPotential statuses of a download in gbif PREPARING, RUNNING, SUCCEEDED, CANCELLED, KILLED, FAILED, SUSPENDED, FILE_ERASED\n'

``` python
import time

def download_wait(key, freqTest=60):
  currentStatus = download_status(key)
  print("status:", currentStatus)
  while currentStatus != 'SUCCEEDED':
    time.sleep(freqTest)
    previousStatus = currentStatus
    currentStatus = download_status(key)
    if currentStatus in ['CANCELLED', 'KILLED', 'FAILED', 'SUSPENDED', 'FILE_ERASED']:
      raise Exception("The key do not correspond to a dowloadable status: " + currentStatus)
    if currentStatus != previousStatus:
      print("status:", currentStatus)
  return key

download_wait(get_query_key(query))
```

    status: SUCCEEDED
    '0002247-260409193756587'

``` python
query = "SELECT gbifid, ScientificName, countryCode FROM occurrence WHERE genus='Espeletia' LIMIT 2"
if not is_in_my_download_list(query):
  key = occ.download_sql(query)
else:
  key=download_wait(get_query_key(query))
```

    status: SUCCEEDED
