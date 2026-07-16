import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
load_dotenv()

pathfile=os.getenv("FILE_SPAT_RESERVAS")

gdf_reservas=gpd.read_file(pathfile)
gdf_reservas=gdf_reservas.rename(columns={'OBJECTID':'id', 'geometry': 'geom'}).set_geometry('geom')
gdf_reservas

engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_reservas.to_postgis("RESERVAS",engine, if_exists='replace')
with engine.begin() as conn:
    conn.execute(text(
        'CREATE INDEX "sidx_RESERVAS_geom" ON public."RESERVAS" USING gist (geom)'
    ))
    
engine.dispose()
