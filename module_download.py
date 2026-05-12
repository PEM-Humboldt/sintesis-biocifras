# Autor: Marius Bottin (github.com/marbotte)

from dotenv import load_dotenv, dotenv_values, find_dotenv
import utils.download as d
load_dotenv(find_dotenv())


def reInitializeGbifFiles(which=['predicates','sql']):
  if 'predicates' in which:
    d.download_gbif_predicates_files(maximum_time_s=0)
  if 'sql' in which:
    d.download_gbif_sql_files(maximum_time_s=0)
  return True


def downloadGbifIfNeeded(which=['predicates','sql']):
  if 'predicates' in which:
    d.download_gbif_predicates_files()
  if 'sql' in which:
    d.download_gbif_sql_files()
  return True

