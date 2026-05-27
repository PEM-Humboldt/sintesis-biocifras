import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import geoalchemy2
import os
load_dotenv()



#TODO: pass that as env variables
pathfile=os.getenv("FILE_SPAT_NARINO_MARITIMO")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/Narino_maritimo/"
#shpFile = "Narino_maritimo_WGS84.shp"
gdf_nm=gpd.read_file(pathfile)
gdf_nm=gdf_nm.rename(columns={'geometry': 'geom'}).set_geometry('geom')
gdf_nm.geom = gdf_nm.geom.to_crs(4326)
#gdf_nm.geom = gdf_nm.geom.to_multipolygon()
gdf_nm.insert(0, "id", [i+1 for i in range(gdf_nm.shape[0])], True)

dropColumns=['shape_Leng','Shape_Leng','shape_Area','mpio_narea','FID_AreaEs', 'OBJECTID', 'Area', 'Shape_Le_1', 'Shape_Le_2','Shape_Le_1', 'Shape_Le_2', 'FID_UAC_LL', 'OBJECTID_2','FID_Limite', 'OBJECTID_3','SHAPE_Le_3', 'SHAPE_Ar_1', 'Area_ha','Shape_Le_4', 'Shape_Area']
for col in list(gdf_nm.columns):
  if col in dropColumns:
    gdf_nm=gdf_nm.drop(labels=[col],axis=1)


engine=create_engine("postgresql://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
gdf_nm.to_postgis("NARINO_MARITIME_REGION",engine, if_exists='replace')

#TODO: define "id" as a primary key



engine.dispose()
