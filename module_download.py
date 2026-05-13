# Autor: Marius Bottin (github.com/marbotte)

from dotenv import load_dotenv, dotenv_values, find_dotenv
import utils.download as d
load_dotenv(find_dotenv())


def reInitializeGbifFiles(which=['predicates','sql']):
  if 'predicates' in which:
    d.download_gbif_predicates_files(predicates=d.default_predicates, maximum_time_s=0)
  if 'sql' in which:
    d.download_gbif_sql(query=d.default_sql_query, maximum_time_s=0)
  return True


def downloadGbifIfNeeded(which=['predicates','sql']):
  if 'predicates' in which:
    d.download_gbif_predicates_files(predicates=d.default_predicates)
  if 'sql' in which:
    d.download_gbif_sql(query=d.default_sql_query)
  return True

