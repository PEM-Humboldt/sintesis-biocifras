import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
load_dotenv()

pathfile=os.getenv("FILE_SPAT_REGION_AMAZONIA")

gdf_region_amazonia=gpd.read_file(pathfile, layer='region-amazonia-departamentos')
gdf_region_amazonia=gdf_region_amazonia.rename(columns={'OBJECTID':'id', 'geometry': 'geom'}).set_geometry('geom')
gdf_region_amazonia

engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_region_amazonia.to_postgis("REGION_AMAZONIA",engine, if_exists='replace')

with engine.begin() as conn:
    conn.execute(text(
        'CREATE INDEX "sidx_REGION_AMAZONIA_geom" ON public."REGION_AMAZONIA" USING gist (geom)'
    ))

engine.dispose()
