import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
load_dotenv()

pathfile=os.getenv("FILE_SPAT_NUCLEOS_DFYB")

gdf_nucleos_dfyb=gpd.read_file(pathfile)
gdf_nucleos_dfyb=gdf_nucleos_dfyb.rename(columns={'OBJECTID':'id', 'geometry': 'geom'}).set_geometry('geom')
gdf_nucleos_dfyb

engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_nucleos_dfyb.to_postgis("NUCLEOS_DFYB",engine, if_exists='replace')
with engine.begin() as conn:
    conn.execute(text(
        'CREATE INDEX "sidx_NUCLEOS_DFYB_geom" ON public."NUCLEOS_DFYB" USING gist (geom)'
    ))
    
engine.dispose()

