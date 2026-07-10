import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
load_dotenv()

pathfile=os.getenv("FILE_SPAT_RESGUARDOS")

gdf_resguardos=gpd.read_file(pathfile)
gdf_resguardos=gdf_resguardos.rename(columns={'OBJECTID':'id', 'geometry': 'geom'}).set_geometry('geom')
gdf_resguardos

engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_resguardos.to_postgis("RESGUARDOS",engine, if_exists='replace')

engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_resguardos.to_postgis("RESGUARDOS",engine, if_exists='replace')
with engine.begin() as conn:
    conn.execute(text(
        'CREATE INDEX "sidx_RESGUARDOS_geom" ON public."RESGUARDOS" USING gist (geom)'
    ))
    
engine.dispose()
