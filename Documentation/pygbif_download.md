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
import requests
load_dotenv()
```

    True

``` python
print(os.getenv("GBIF_USER"))
```

    bottinmarius

``` python
#occ.download_sql("SELECT gbifid, ScientificName, countryCode FROM occurrence WHERE genus='Espeletia' LIMIT 10")
```

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
‘request.format’, ‘request.type’, ‘request.checklistKey’,
‘request.predicate.type’, ‘request.predicate.predicates’,
‘request.verbatimExtensions’, ‘request.interpretedExtensions’\],
dtype=‘str’)

``` python
df_downList2 = df_downList[['key', 'doi','created','status','request.sendNotification','downloadLink']]
print(tabulate(df_downList2, headers = 'keys', tablefmt = 'github'))
```

|  | key | doi | created | status | request.sendNotification | downloadLink |
|----|----|----|----|----|----|----|
| 0 | 0004286-260806074905277 | 10.15468/dl.qe2d2x | 2026-08-08T16:58:13.596+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0004286-260806074905277.zip |
| 1 | 0004170-260806074905277 | 10.15468/dl.m9k9fp | 2026-08-08T14:25:32.885+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0004170-260806074905277.zip |
| 2 | 0003420-260806074905277 | 10.15468/dl.xbnznz | 2026-08-08T03:17:38.940+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0003420-260806074905277.zip |
| 3 | 0003414-260806074905277 | 10.15468/dl.rs67zs | 2026-08-08T03:15:52.111+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0003414-260806074905277.zip |
| 4 | 0003361-260806074905277 | 10.15468/dl.y9vdqa | 2026-08-08T02:35:01.949+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0003361-260806074905277.zip |
| 5 | 0003335-260806074905277 | nan | 2026-08-08T02:09:32.414+00:00 | CANCELLED | True | https://api.gbif.org/v1/occurrence/download/request/0003335-260806074905277.zip |
| 6 | 0003324-260806074905277 | nan | 2026-08-08T01:56:38.418+00:00 | CANCELLED | False | https://api.gbif.org/v1/occurrence/download/request/0003324-260806074905277.zip |
| 7 | 0003217-260806074905277 | 10.15468/dl.gep3k5 | 2026-08-08T00:02:05.794+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0003217-260806074905277.zip |
| 8 | 0003094-260806074905277 | 10.15468/dl.r5nr4r | 2026-08-07T22:04:06.314+00:00 | SUCCEEDED | False | https://api.gbif.org/v1/occurrence/download/request/0003094-260806074905277.zip |
| 9 | 0003091-260806074905277 | nan | 2026-08-07T22:02:57.548+00:00 | CANCELLED | False | https://api.gbif.org/v1/occurrence/download/request/0003091-260806074905277.zip |
| 10 | 0003058-260806074905277 | nan | 2026-08-07T21:29:03.149+00:00 | CANCELLED | False | https://api.gbif.org/v1/occurrence/download/request/0003058-260806074905277.zip |
| 11 | 0003035-260806074905277 | nan | 2026-08-07T21:15:39.317+00:00 | CANCELLED | False | https://api.gbif.org/v1/occurrence/download/request/0003035-260806074905277.zip |
| 12 | 0001181-260806074905277 | 10.15468/dl.7qvhjm | 2026-08-06T20:55:46.084+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0001181-260806074905277.zip |
| 13 | 0000136-260721160103020 | 10.15468/dl.qfyxpd | 2026-07-21T17:19:06.172+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0000136-260721160103020.zip |
| 14 | 0028327-260519110011954 | 10.15468/dl.qanw77 | 2026-06-01T14:06:56.911+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0028327-260519110011954.zip |
| 15 | 0012748-260507073636908 | 10.15468/dl.cmt5s9 | 2026-05-12T13:11:29.093+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0012748-260507073636908.zip |
| 16 | 0012747-260507073636908 | nan | 2026-05-12T13:11:12.562+00:00 | CANCELLED | True | https://api.gbif.org/v1/occurrence/download/request/0012747-260507073636908.zip |
| 17 | 0012707-260507073636908 | nan | 2026-05-12T12:58:35.976+00:00 | CANCELLED | True | https://api.gbif.org/v1/occurrence/download/request/0012707-260507073636908.zip |
| 18 | 0011985-260507073636908 | 10.15468/dl.6a45kb | 2026-05-12T05:12:59.862+00:00 | SUCCEEDED | True | https://api.gbif.org/v1/occurrence/download/request/0011985-260507073636908.zip |
| 19 | 0011984-260507073636908 | nan | 2026-05-12T05:12:21.631+00:00 | CANCELLED | True | https://api.gbif.org/v1/occurrence/download/request/0011984-260507073636908.zip |

Podemos utilizar esta lista para probar si ya se descargo, a través de
la API SQL una consulta SQL (Nota: imagino que cuando son consultas
complejas, podríamos tener problemas con los cambios de linea):

``` python
def is_in_my_download_list(sql_query, limit=100):
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
def get_query_key(sql_query, limit=100):
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

## Crear la consulta geográfica con el WKT

Para manejar datos espaciales vectoriales, los paquetes Python más
utilizados son:

- `shapely` que parece ser la base de codigo de muchos otros paquetes,
  `shapely` soló no contiene funciones de lectura de shapefiles…
- `fiona` que parece ser una solución un poco más completa, pero algunos
  usuarios han mencionado dificultades de instalación
- `geopandas` que permite mezclar las posibilidades de `pandas` y
  `shapely`, pero que es una dependencia particularmente pesada

``` python
import fiona
import shapely
from shapely.geometry import shape
#from shapely.geometry import shape,Polygon, MultiPolygon

with fiona.open("../../data_sintesis-biocifras/RegionesMaritimas.shp") as src:
    for feature in src:
        # Convert the record geometry to a Shapely object
        geom_rm = shape(feature['geometry'])
        print(geom_rm.geom_type)
```

    Polygon
    Polygon
    Polygon
    Polygon
    Polygon

``` python
        

with fiona.open("../../data_sintesis-biocifras/MGN_DPTO_POLITICO_2023.shp") as src:
    for feature in src:
        # Convert the record geometry to a Shapely object
        geom_col = shape(feature['geometry'])
        print(geom_col.geom_type)
```

    Polygon
    Polygon
    Polygon
    MultiPolygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    MultiPolygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon
    MultiPolygon
    Polygon
    Polygon
    Polygon
    MultiPolygon
    Polygon
    Polygon
    Polygon
    Polygon
    Polygon

``` python
geom_multi_rm = shapely.unary_union(geom_rm)
geom_multi_col = shapely.unary_union(geom_col)
geom_multi = shapely.union(geom_rm,geom_col)
geom_polys = list(geom_multi.geoms)

exportPath = "../../data_sintesis-biocifras/"
filerm = exportPath + 'geom_rm.geojson'
filecol = exportPath + 'geom_col.geojson'
fileGeomMulti=exportPath+'geom_multi.geojson'
with open(filerm,'w') as f:
  f.write(shapely.to_geojson(geom_multi_rm))
```

    6893

``` python
with open(filecol,'w') as f:
  f.write(shapely.to_geojson(geom_multi_col))
```

    1108073

``` python
with open(fileGeomMulti,'w') as f:
  f.write(shapely.to_geojson(geom_multi))
```

    1114941

Parece que el manejo de los datos espaciales desde los paquetes
`shapely` y `fiona`, aunque parecen ser la solución más ligera, son
demasiado diferentes de lo que conozco en el paquete `sf` de R para que
yo pueda adaptar mis codigos de manera rapida: `geopandas`, aunque más
pesado, tiene una documentación mucho más facil y una logica más
parecida a `sf`

``` python
import matplotlib.pyplot as plt
import geopandas as gpd
datadir = "../../data_sintesis-biocifras/"
df_rm = gpd.read_file(datadir + 'RegionesMaritimas.shp')
df_rm.plot(color='lightblue', edgecolor='black');
plt.show()
```

![](./Fig/pygbifunnamed-chunk-9-1.png)

``` python
df_rm_un = df_rm.dissolve()
df_rm_un.plot(color='lightblue', edgecolor='black')
plt.show()
```

![](./Fig/pygbifunnamed-chunk-10-3.png)

``` python
df_col=gpd.read_file(datadir + 'MGN_MPIO_POLITICO_2023.shp')
df_col.plot()
plt.show()
```

![](./Fig/pygbifunnamed-chunk-11-5.png)

``` python
df_col_un = df_col.dissolve()
df_col_un.plot(color='lightblue', edgecolor='black')
plt.show()
```

![](./Fig/pygbifunnamed-chunk-12-7.png)

``` python
allCol=df_col_un.union(df_rm_un)
allCol.plot(edgecolor='black')
plt.show()
```

![](./Fig/pygbifunnamed-chunk-13-9.png)

Entonces, logramos las operaciones dissolve y union para crear la
geometría grande de colombia y de su zona maritima, sin embargo, al
nivel de precisión de las capas, nos toca todavía supprimir los “inner
holes” del poligono obtenido

``` python
from shapely.geometry import Polygon
def remove_interiors(poly):
    """
    Close polygon holes by limitation to the exterior ring.

    Arguments
    ---------
    poly: shapely.geometry.Polygon
        Input shapely Polygon

    Returns
    ---------
    Polygon without any interior holes
    """
    if poly.interiors:
        return Polygon(list(poly.exterior.coords))
    else:
        return poly
polygon=allCol.geometry[0]
ser=remove_interiors(polygon)
gdf=gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[ser])
gdf.plot(edgecolor='black')
plt.show()
```

![](./Fig/pygbifunnamed-chunk-14-11.png)

Para poder enviar esta geometría en una consulta SQL de la API de GBIF,
se tiene que simplificar. Existen operaciones especificas del paquete
`shapely` para este objetivo:

``` python
ser2=ser.simplify(0.0006)
gdf=gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[ser2])
gdf.plot(edgecolor='black')
plt.show()
```

![](./Fig/pygbifunnamed-chunk-15-13.png)

Longitud de los wkt antes y después de la simplificación

``` python
len(ser.wkt)
```

    4918808

``` python
len(ser2.wkt)
```

    342127

Una manera que puede ser más correcta de representar la complejidad es
contar el numero de coma en las representaciones WKT, que corresponde al
numero de puntos -1 .

``` python
ser.wkt.count(",")
```

    127318

``` python
ser2.wkt.count(",")
```

    8880

Una de las recomendaciones de GBIF es integrar unas condiciones de
coordenadas máximas y mínimas para facilitar el proceso biológico.

``` python
minx=ser2.bounds[0]
miny=ser2.bounds[1]
maxx=ser2.bounds[2]
maxy=ser2.bounds[3]
```

Ahora simplemente se utilizan los parámetros para construir la consulta
para la API de GBIF.

``` python
query="SELECT countrycode,hasgeospatialissues,count(*) FROM occurrence WHERE countrycode='CO' OR (decimalLatitude <= "+ str(maxy) +" AND decimalLatitude >= " + str(miny) + " AND decimalLongitude <= " + str(maxx) +  "AND decimalLongitude >= " + str(minx) + "  AND GBIF_WITHIN('" + ser2.wkt + "', decimalLatitude, decimalLongitude)) " + "GROUP BY countrycode, hasgeospatialissues"
with open(datadir + 'spatialQuery1.sql','w') as f:
  f.write(query)
```

    342461

``` python
if is_in_my_download_list(query):
   key=download_wait(get_query_key(query))
else:
    key = download_wait(occ.download_sql(query))
```

    status: SUCCEEDED

En particular nos interesa poder mirar los registros que no tiene ‘CO’
como countryCode

``` python
query = "SELECT gbifid,scientificname, datasetid, countrycode,hasgeospatialissues, decimalLatitude, decimalLongitude FROM occurrence WHERE (countrycode IS NULL OR countrycode <> 'CO') AND decimalLatitude <= "+ str(maxy) +" AND decimalLatitude >= " + str(miny) + " AND decimalLongitude <= " + str(maxx) +  "AND decimalLongitude >= " + str(minx) + "  AND GBIF_WITHIN('" + ser2.wkt + "', decimalLatitude, decimalLongitude) " 
with open(datadir + 'spatialQuery2.sql','w') as f:
  f.write(query)
```

    342507

``` python
if is_in_my_download_list(query):
  key=download_wait(get_query_key(query))
else:
  key = download_wait(occ.download_sql(query))
```

    status: SUCCEEDED

Ahora para descargar efectivamente el archivo zip:

``` python
downloaded_query2 = occ.download_get(key, path = datadir)
```

Por ahora, para poder avanzar rapido, voy a seguir en R

``` r
library(readr)
library(zip)
```


    Attaching package: 'zip'

    The following objects are masked from 'package:utils':

        unzip, zip

``` r
zipFile <- reticulate::py$downloaded_query2$path
contents <- zip_list(zipFile)
data_query2<-read.csv(unz(zipFile,contents$filename[1]),row.names = NULL,sep="\t")
head(data_query2)
```

| gbifid | scientificname | datasetid | countrycode | hasgeospatialissues | decimallatitude | decimallongitude |
|---:|:---|:---|:---|:---|---:|---:|
| 4075426156 | Salix nigra Marshall |  | US | true | 11.033333 | -75.54694 |
| 4075427333 | Sarracenia purpurea L. |  | US | true | 3.776639 | -75.33017 |
| 1142263949 | Polystira Woodring, 1928 | invertebrates-19-mar-2026 | VE | true | 12.520000 | -71.68000 |
| 1142263922 | Polystira albida (G.Perry, 1811) | invertebrates-19-mar-2026 | VE | true | 12.430000 | -71.93000 |
| 1065259260 | Strophocheilus Spix, 1827 | invertebrates-19-mar-2026 | EC | true | 2.066700 | -75.80000 |
| 1142263942 | Polystira albida (G.Perry, 1811) | invertebrates-19-mar-2026 | VE | true | 12.520000 | -71.68000 |

``` r
require(sf)
```

    Loading required package: sf

    Linking to GEOS 3.13.0, GDAL 3.13.1, PROJ 9.4.1; sf_use_s2() is TRUE

``` r
DSN <- "../../data_sintesis-biocifras/"
reg_mar<-st_read(dsn=DSN,layer = "RegionesMaritimas")
```

    Reading layer `RegionesMaritimas' from data source 
      `/home/marius/Travail/traitementDonnees/2026_scripts_filter_sintesis_cifras/data_sintesis-biocifras' 
      using driver `ESRI Shapefile'
    Simple feature collection with 5 features and 2 fields
    Geometry type: MULTIPOLYGON
    Dimension:     XY
    Bounding box:  xmin: -85.9926 ymin: 1.429 xmax: -69.4917 ymax: 16.1694
    Geodetic CRS:  WGS 84

``` r
depto<-st_read(dsn=DSN, layer="MGN_DPTO_POLITICO_2023")
```

    Reading layer `MGN_DPTO_POLITICO_2023' from data source 
      `/home/marius/Travail/traitementDonnees/2026_scripts_filter_sintesis_cifras/data_sintesis-biocifras' 
      using driver `ESRI Shapefile'
    Simple feature collection with 33 features and 9 fields
    Geometry type: MULTIPOLYGON
    Dimension:     XY
    Bounding box:  xmin: -81.73562 ymin: -4.229406 xmax: -66.84722 ymax: 13.39473
    Geodetic CRS:  WGS 84

``` r
dq2_s<-st_as_sf(data_query2,coords=c("decimallongitude","decimallatitude"))
par(mar=rep(.5,4))
plot(c(st_geometry(reg_mar),st_geometry(depto)), col=NA, border=NA)
plot(st_geometry(reg_mar), col="lightblue", border="white",add=T)
plot(st_geometry(depto), col="lightgreen", border="white",add=T)
plot(st_geometry(dq2_s[!(is.na(dq2_s$countrycode)|dq2_s$countrycode==""),]),col="orange",add=T, pch=16,cex=.5)
plot(st_geometry(dq2_s[is.na(dq2_s$countrycode)|dq2_s$countrycode=="",]),col="red",add=T, pch=16,cex=.5)
legend("topright",fill=c("red","orange"),legend=c("Ningun countrycode","countrycode diferente a 'CO'"), cex=.7)
```

![](./Fig/pygbifunnamed-chunk-23-1.png)

``` r
A<-table(is.na(dq2_s$countrycode)|dq2_s$countrycode=="")
data.frame(`No countrycode`=names(A),registros=as.numeric(A))
```

| No.countrycode | registros |
|:---------------|----------:|
| FALSE          |     27081 |
| TRUE           |     10095 |

``` r
A <- table(dq2_s$datasetid[is.na(dq2_s$countrycode)|dq2_s$countrycode==""])
data.frame(dataset=names(A),registros=as.numeric(A))
```

| dataset                                         | registros |
|:------------------------------------------------|----------:|
|                                                 |     10019 |
| 191                                             |         3 |
| 203                                             |         3 |
| 211                                             |         1 |
| 885                                             |        23 |
| calcofi.io_workflows_ichthyo_to_obis_2026-03-06 |        46 |

## Utilizar y controlar el backbone

Cuando empezamos este proyecto, el único backbone de taxonomía
disponible en GBIF era el “GBIF-Backbone”. Poco a poco, GBIF está
adoptando otro sistema taxonomico “COL-XR” (Catalog of Life Extended
Release).

Desafortunadamente, al momento de probar y comparar las funcionalidades
del sistema para su entrega al instituto Humboldt, estamos en una
situación complicada donde la nueva norma es utilizar COL-XR, pero el
desarrollo de las funcionalidades desarrolladas en GBIF no está
exactamente claro ni terminado…

Lo que vamos a probar, y describir en esta sección cambiará
probablemente en el año 2026.

Al momento de escribir esas líneas, no existe la posibilidad de utilizar
el paquete `pygbif` y sus funciones con un parametro simple para
descargar datos que utilizan un backbone o otro. No quiere decir que no
existan soluciones, simplemente que complican un poco la forma de
interactuar con las APIs de descarga de los datos.

### Controlar el backbone desde la api de GBIF “Clasica”

Tomamos el ejemplo de la descarga de los registros en Colombia del
genero *Acer*, permite obtener suficientes datos, pero no hacer un
download masivo de datos.

``` python
USER=os.getenv("GBIF_USER")
PASS=os.getenv("GBIF_PWD")
EMAIL=os.getenv("GBIF_EMAIL")
payload = {
    "creator": USER,
    "notificationAddresses": [EMAIL],
    "format": "DWCA",
 "predicate": {
  "type": "and",
  "predicates": [
    {
      "type": "equals",
      "key": "OCCURRENCE_STATUS",
      "value": "present",
      "matchCase": False
    },
    {
      "type": "equals",
      "key": "COUNTRY",
      "value": "CO",
      "matchCase": False
    },
    {
      "type": "in",
      "key": "TAXON_KEY",
      "values": ["64FK9","946Z","949X","94DL","94F5","94G9","94H3","94HF","94JK","MLD"],
      "checklistKey": "7ddf754f-d193-4cc9-b351-99906754a03b"
    }
  ]
},
 "checklistKey": "7ddf754f-d193-4cc9-b351-99906754a03b"

}    




r = requests.post(
    "https://api.gbif.org/v1/occurrence/download/request",
    json=payload,
    auth=(USER, PASS),
)

print(r.status_code, r.text)
```

    201 0004540-260806074905277

### Controlar el backbone desde la API SQL

Intentemos construir algo parecido con la API SQL:

``` python
backbone = os.getenv("TAXONOMIC_BACKBONE","COL_XR")
backbone_uuids = {'GBIF_BACKBONE': 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c', 'COL_XR': '7ddf754f-d193-4cc9-b351-99906754a03b'}
checklistKey = backbone_uuids.get(backbone)

sql_query = f'''SELECT gbifid, 
      occurrenceid,
      basisofrecord,
      collectioncode,
      catalognumber,
      recordedby,
      individualcount,
      eventdate,
      countrycode,
      stateprovince,
      locality,
      elevation,
      depth,
      decimallatitude,
      decimallongitude,
      coordinateuncertaintyinmeters,
      classificationdetails[\'{checklistKey}\'][\'scientificname\'] scientificname,
      classificationdetails[\'{checklistKey}\'][\'kingdom\'] AS kingdom,
      classificationdetails[\'{checklistKey}\'][\'phylum\'] AS phylum,
      classificationdetails[\'{checklistKey}\'][\'class\'] AS class,
      classificationdetails[\'{checklistKey}\'][\'order\'] AS "order",
      classificationdetails[\'{checklistKey}\'][\'family\'] AS family,
      classificationdetails[\'{checklistKey}\'][\'genus\'] AS genus,
      classificationdetails[\'{checklistKey}\'][\'species\'] AS species,
      classificationdetails[\'{checklistKey}\'][\'infraspecificepithet\'] AS infraspecificepithet,
      classificationdetails[\'{checklistKey}\'][\'taxonrank\'] AS taxonrank,
      "day",
      "month",
      "year",
      classificationdetails[\'{checklistKey}\'][\'verbatimscientificname\'] AS v_scientificname,
      datasetkey,
      publishingorgkey,
      classificationdetails[\'{checklistKey}\'][\'taxonkey\'] AS taxonkey,
      issue,
      occurrencestatus,
      lastinterpreted,
      type,
      datasetid,
      datasetname,
      organismquantity,
      organismquantitytype,
      eventid,
      samplingprotocol,
      county,
      municipality,
      repatriated,
      publishingcountry,
      lastparsed FROM occurrence WHERE occurrence.countrycode = 'CO' AND occurrence.occurrencestatus = 'PRESENT' AND  classificationdetails[\'{checklistKey}\'][\'genus\'] = \'Acer\''''
occ.download_sql(sql_query)
```

    '0004541-260806074905277'
