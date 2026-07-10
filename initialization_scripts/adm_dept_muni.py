import requests
import re
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import geoalchemy2
import os
from zipfile import ZipFile
load_dotenv()



url = os.getenv("URL_MGN_DEPT_MUNI")
dirzip = os.getenv("DIR_DOWNLOAD_ZIP")
zipName="MGN_MPIO_POLITICO.zip"


r=requests.get(url)
with open(dirzip+zipName, 'wb') as file:
        file.write(r.content)


#TODO: pass that as env variables
datadir = os.getenv("EXTDATADIR")+"/adm_dept_muni/"

if not os.path.exists(datadir):
  os.makedirs(datadir)

with ZipFile(dirzip+zipName, 'r') as zObject:
    listFiles_zip=zObject.namelist()
    zObject.extractall(path=datadir)

shpFile=[f for f in listFiles_zip if re.search(".*.shp$",f)][0]

gdf_adm=gpd.read_file(datadir+shpFile)
gdf_adm=gdf_adm.rename(columns={'geometry': 'geom'}).set_geometry('geom')
gdf_adm.geom = gdf_adm.geom.to_crs(4326)
gdf_adm.insert(0, "id", [i+1 for i in range(gdf_adm.shape[0])], True)

dropColumns=['shape_Leng','shape_Area','mpio_narea']
for col in list(gdf_adm.columns):
  if col in dropColumns:
    gdf_adm=gdf_adm.drop(labels=[col],axis=1)


engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_adm.to_postgis("MGN_ADM_MPIO_2025",engine, if_exists='replace')

#TODO: define "id" as a primary key
#TODO: eliminate 2025 from the name so it can work later

with engine.begin() as conn:
    conn.execute(text(
        'CREATE INDEX "sidx_MGN_ADM_MPIO_2025_geom" ON public."MGN_ADM_MPIO_2025" USING gist (geom)'
    ))

engine.dispose()

