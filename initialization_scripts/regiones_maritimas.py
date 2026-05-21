import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import geoalchemy2
import os
load_dotenv()



#TODO: pass that as env variables
datadir = "../../data_sintesis-biocifras/fuentesExternas/RegionesMaritimas/"
file="RegionesMaritimas.shp"


gdf_RM=gpd.read_file(datadir+file)
#TODO: el nombre "DESCRIP" no es una buena idea para el nombre de columna en Postgres, pero cambiar este nombre se debe hacer en todos los tratamientos que utilizan esta tabla
gdf_RM=gdf_RM.rename(columns={'OBJECTID':'id', 'geometry': 'geom'}).set_geometry('geom')
gdf_RM


engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_RM.to_postgis("INVEMAR_MARITIME_REGIONS",engine, if_exists='replace')

#TODO: define "id" as a primary key



engine.dispose()
