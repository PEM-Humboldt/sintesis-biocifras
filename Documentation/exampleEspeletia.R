library(rgbif)
query<-"SELECT gbifid, ScientificName, countryCode FROM occurrence WHERE genus='Espeletia' LIMIT 10"
test <- occ_download_sql_prep(query)
(espeletia<-occ_download_sql(query))
occ_download_wait(espeletia)
(res<-occ_download_get(espeletia) %>% occ_download_import())
