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

pathfile=os.getenv("FILE_STATS_ESTIMATED_DEPT")

list_estimated_dept=pd.read_csv(pathfile, sep=";",low_memory=False)
list_estimated_dept=list_estimated_dept.rename(columns={i: i.lower() for i in list(list_estimated_dept.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(list_estimated_dept.columns):
  if col in dropColumns:
    list_estimated_dept=list_estimated_dept.drop(labels=[col],axis=1)

db=c.get_db()

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
list_estimated_dept.to_sql("tmp_cifras_estimated_dept",engine, if_exists='replace', index=False)
engine.dispose()
    
db.dispose()
  


