# Autor: Marius Bottin (github.com/marbotte)

import os
import json
from pygbif import occurrences as occ
import pandas as pd
import time
from datetime import datetime, timezone
from dateutil import parser
from zipfile import ZipFile
################################################################################
### PARTE 1: Funciones genericas
################################################################################

default_predicates={'type':'and', 'predicates':[{'type': 'equals', 'key': 'COUNTRY', 'value': 'CO','matchCase': False}, {'type': 'equals', 'key': 'OCCURRENCE_STATUS', 'value': 'present', 'matchCase':False}]}

default_sql_query='SELECT gbifid, occurrenceid, basisofrecord, collectioncode, catalognumber, recordedby, individualcount, eventdate, countrycode, stateprovince, locality, elevation, depth, decimallatitude, decimallongitude, coordinateuncertaintyinmeters, scientificname, kingdom, phylum, class, "order", family, genus, species, infraspecificepithet, taxonrank, "day", "month", "year", v_scientificname, datasetkey, publishingorgkey, taxonkey, issue, occurrencestatus, lastinterpreted, type, datasetid, datasetname, organismquantity, organismquantitytype, eventid, samplingprotocol, county, municipality, repatriated, publishingcountry, lastparsed' + ' FROM occurrence' + " WHERE occurrence.countrycode = 'CO' AND occurrence.occurrencestatus = 'PRESENT'"

def corresponding_download_list(format_download, predicates = default_predicates, sql_query=default_sql_query, maximum_time_s = 60*60*24*7, limit = 20, user=os.getenv("GBIF_USER"), pwd=os.getenv("GBIF_PWD")):
  accepted_formats=['DWCA','SIMPLE_CSV','SQL_TSV_ZIP']
  if not format_download in accepted_formats:
    raise Exception(ValueError,format_download + ' is not in the accepted download formats')
  downList = occ.download_list(user=user,pwd=pwd,limit=limit)
  df_downList = pd.json_normalize(downList["results"])
  df_downList['created_seconds_from_now']=[(datetime.now(timezone.utc) - parser.parse(r['created'])).total_seconds() for i,r in df_downList.iterrows()]
  if format_download == 'SQL_TSV_ZIP':
    tableOK=df_downList.query("created_seconds_from_now < @maximum_time_s and `request.sql` == @sql_query and (status == 'PREPARING' or status == 'RUNNING' or status == 'SUCCEEDED')")
  else:
    df_downList['completePredicates']=[{'type':r['request.predicate.type'],'predicates':r['request.predicate.predicates']} for i,r in df_downList.iterrows()]
    tableOK=df_downList.query("completePredicates == @predicates and created_seconds_from_now < @maximum_time_s and `request.format` == @format_download and (status == 'PREPARING' or status == 'RUNNING' or status == 'SUCCEEDED')")
  tableOK=tableOK.sort_values(by='created_seconds_from_now', ascending=True)
  return tableOK

def download_status(key):
  status = occ.download_meta(key)['status']
  return status
  

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

def zip_exists(key, dir_zip=os.getenv("DIR_DOWNLOAD_ZIP")):
  return os.path.exists(dir_zip + key + '.zip')

def download_zip(key, checkExists=True, dir_zip=os.getenv("DIR_DOWNLOAD_ZIP")):
  zipPath=dir_zip + key + '.zip'
  if (checkExists and not zip_exists(key,dir_zip=dir_zip)) or not checkExists:
    occ.download_get(key, path=dir_zip)
  return zipPath

def extract_gbifZip(zipFile, nameInZip, destFile):
  dirzip=os.path.dirname(zipFile)
  with ZipFile(zipFile,mode="r") as archive:
    zipContent=archive.namelist()
    if not nameInZip in zipContent:
      raise Exception(ValueError, nameInZip + ' is not in the zip file')
    archive.extract(nameInZip, dirzip)
  os.rename(os.path.join(dirzip,nameInZip),destFile)
  return destFile


################################################################################
### PARTE 2: descargar datos con la api classica de GBIF ###
################################################################################

def download_gbif_predicates_files(predicates=default_predicates, maximum_time_s = 60*60*24*7, maximum_diff_time_s=60*60, limit = 20,  simple_csv_file=os.getenv('OCCURRENCE_FILE'), verbatim_dwca_file=os.getenv('VERBATIM_FILE'), user=os.getenv("GBIF_USER"), pwd=os.getenv("GBIF_PWD")):
  dwca_corres=corresponding_download_list(format_download= 'DWCA', predicates=predicates, maximum_time_s=maximum_time_s, limit=limit, user=user, pwd=pwd )
  simple_corres=corresponding_download_list(format_download='SIMPLE_CSV', predicates=predicates, maximum_time_s=maximum_time_s, limit=limit, user=user, pwd=pwd)
  preparedDownloadExists = dwca_corres.shape[0] > 0 and simple_corres.shape[0] > 0
  if preparedDownloadExists:
    timeDiffBetweenFiles=abs(dwca_corres['created_seconds_from_now'].values[0] - simple_corres['created_seconds_from_now'].values[0])
    if timeDiffBetweenFiles > maximum_diff_time_s:
      print("Too much time difference between simple and dwca download")
      needNew=True
    else:
      needNew=False
  else:
    print("Not all downloads have already been prepared")
    needNew=True
  if needNew:
    print('New downloads need to be prepared')
    dwca_key = occ.download(predicates,'DWCA',user=user,pwd=pwd)[0]
    simple_key = occ.download(predicates, 'SIMPLE_CSV',user=user,pwd=pwd)[0]
  else:
    print('Downloads are already prepared in user\'s GBIF API')
    dwca_key = dwca_corres['key'].values[0]
    simple_key = simple_corres['key'].values[0]
  download_wait(simple_key)
  download_wait(dwca_key)
  print("Dowloading zip files")
  simple_zip = download_zip(simple_key)
  dwca_zip = download_zip(dwca_key)
  print("Extracting files")
  simple_file = extract_gbifZip(simple_zip, nameInZip = simple_key + '.csv', destFile=simple_csv_file)
  verbatim_file = extract_gbifZip(dwca_zip, nameInZip ='verbatim.txt', destFile=verbatim_dwca_file)
  return [simple_file, verbatim_file]
    

###############################################################################
### PARTE 3: descargar datos con la api SQL de GBIF ###
################################################################################

def download_gbif_sql(query=default_sql_query, maximum_time_s = 60*60*24*7, limit = 20, sql_file=os.getenv('SQL_FILE'), user=os.getenv("GBIF_USER"), pwd=os.getenv("GBIF_PWD")):
  sql_corres=corresponding_download_list(format_download='SQL_TSV_ZIP', sql_query=query, maximum_time_s=maximum_time_s, limit=limit, user=user, pwd=pwd)
  needNew = (sql_corres.shape[0] == 0)
  if needNew:
    print('New download needs to be prepared')
    sql_key = occ.download_sql(query, user=user, pwd=pwd)[0]
  else:
    print('SQL download is already prepared in user\'s GBIF API')
    sql_key = sql_corres['key'].values[0]
  download_wait(sql_key)
  print("Dowloading zip files")
  sql_zip = download_zip(sql_key)
  print("Extracting file")
  sql_file = extract_gbifZip(sql_zip, nameInZip = sql_key + '.csv', destFile=sql_file)
  return sql_file
    
