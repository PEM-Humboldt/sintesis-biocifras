from dotenv import load_dotenv
import os
import sys
import sqlalchemy as sqlal
import pandas as pd
from psycopg2 import sql
import warnings
load_dotenv()

sys.path.append('..')
import utils.connection as c


datadir = "../../data_sintesis-biocifras/fuentesExternas/"
file = "geo_master_geography_20260525.tsv"

master_geog=pd.read_csv(datadir+file, sep="\t",low_memory=False)
#master_geog=master_geog.rename(columns={i: i.lower() for i in list(master_geog.columns)})
#master_geog=master_geog.rename(columns={"file" : "sourcefile"})

#master_geog.insert(0, "id", [i+1 for i in range(master_geog.shape[0])], True)

dropColumns=['put the names of the columns you wanna suppress here']
for col in list(master_geog.columns):
  if col in dropColumns:
    master_geog=master_geog.drop(labels=[col],axis=1)

colNeeded=['name','slug','type','subtype','codedane','ismarine','description','nameparent','parentparent','region','regionparent']
for col in colNeeded:
  if not col in list(master_geog.columns):
    raise Exception(col + ' column is absent, and needed for the database to work')


engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
master_geog.to_sql("tmp_master_geog",engine, if_exists='replace', index=True)
engine.dispose()


db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS geo_master_geography")
  conn.execute('''
  CREATE TABLE public.geo_master_geography (
    id int PRIMARY KEY,
    name text NOT NULL,
    slug text NOT NULL,
    type text,
    subtype text,
    codedane text,
    ismarine boolean,
    description text,
    parent_id integer REFERENCES geo_master_geography (id),
    region_id integer REFERENCES geo_master_geography (id),
    UNIQUE (name, parent_id)
  );
  ''')
  conn.execute('''WITH a AS(
        SELECT *,
          CASE
            WHEN subtype='pais' THEN 0
            WHEN subtype='departamento' THEN 1
            WHEN subtype='municipio' THEN 2
            ELSE 99
          END::int AS ord
        FROM tmp_master_geog tmg
        ), b AS (
        SELECT ROW_NUMBER() OVER (ORDER BY ord,name) AS id,
          name, slug, type,subtype,codedane,ismarine,description,nameparent,parentparent,region,regionparent
          FROM a
        ), c AS(
        SELECT b1.id, b1.name, b1.slug, b1.type, b1.subtype, b1.codedane, b1.ismarine, b1.description, b2.id AS parent_id, b3.id AS region_id
        FROM b b1
        LEFT JOIN b b2 ON b1.nameparent=b2.name AND b1.parentparent IS NOT DISTINCT FROM b2.nameparent
        LEFT JOIN b b3 ON b1.region=b3.name AND b1.regionparent IS NOT DISTINCT FROM b3.nameparent
        )
        INSERT INTO geo_master_geography
        SELECT *
        FROM c
      ''')
  conn.execute("CREATE INDEX idx_geo_divipola_slug ON public.geo_master_geography USING btree (slug);")
  conn.execute('DROP TABLE IF EXISTS tmp_master_geog')
  conn.commit()
  

db.dispose()
