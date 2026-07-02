# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 09:11:04 2022
edit on Aug 01 2024
@author: camila.plata
@editor: Nerieth Leuro, Natalia Medina
@update: 2026-03-17

Este script fue creado por el SiB Colombia para realizar la sintesis de 
cifras sobre biodiversidad de Colombia para especies y registros 

    
Tenga en cuenta que dentro del script podra encontrar algunas secciones identificadas 
con ##, estas corresponden a líneas explicativas. Y partes del código que se encuentran 
comentariadas con # que corresponden a secciones de código que se pueden habilitar o 
deshabilitar según los datos que se desee obtener.

"""

##-----------------------------------------------------1. Preparar el entorno---------------------------------------------##
##Importar las librerias necesarias para el uso del script
import pandas as pd 
import os 
import time
import numpy as np
#import gc


##Iniciar el conteo de tiempo de ejecución del script
inicio=time.time()
print(inicio)

##Mostrar la carpeta principal donde se encuentra alojado el script
print(os.getcwd())

##Establecer la carpeta de trabajo
##Tenga en cuenta que en el momento de diligenciar la ubicación de la carpeta de trabajo se deben documentar doble barra invertida "\\" para evitar errores de sintaxis
os.chdir("D:\\IAvH\\BioCifras\\2025T4")

##Crear una variable string con el nombre del conjunto de datos. Este nombre debe incluir: la geografía, el año y el corte trimestral utilizado 
fecha_corte='2025-12-31'

##----------------------------------------------------2. Cargar archivos--------------------------------------------------##
'''
Para conjuntos de datos para obtener cifras municipales y equipos con poca memoria RAM (8 gb o menos)
Se agrega dtype (asignar el tipo de dato a cada columna)
Con el fin de evitar el error: pandas.errors.ParserError: Error tokenizing data. C error: out of memory
En el caso de conjuntos de datos para cifras departamentales realice la ejecución en un equipo con memoria RAM de 16 gb

dtype: se especifica cada columna y el tipo de dato que corresponde, el orden de los campos no afecta la ejecución, pero debe actualizarse
cada que se agreguen y quiten campos para hacer más eficiente el proceso.
    str: Se utiliza para campos que contengan texto y símbolos
    float: Aplica en campos que contengan números decimales
Adicionalmente, se especifican las columnas a cargar con el fin de dejar solo las columnas necesarias
para la obtención de cifras y no sobrecargar la memoria RAM y hacer el proceso más eficiente.
'''

##Crear un objeto cargando la tabla de datos(formato tsv o txt)
#registros = pd.read_table('Cortes geográficos/dwc_co_2023-12-31_marinos_MADSAjustados.tsv', sep='\t', encoding = "utf8",dtype=str)    
#registros = pd.read_table('PCV/dwc_co_2023T4_PNN.tsv', sep='\t', encoding = "utf8",dtype=str, nrows=10000))   
registros = pd.read_table('D:\\IAvH\\Corte trimestral\\2025\\2025T4\\dwc_co_2025-12-31_v2.txt', sep='\t', encoding = "utf8",dtype=str)   
#print(registros.columns)
#registros = registros.rename(columns={"species_x": "species"})
#registros = registros.rename(columns={"especies_exotica_riesgo_invasion_total": "especies_exotica_riesgo_invasion"})
#registros['especies_exotica_riesgo_invasion'] = registros.apply(lambda row: str(row['especies_exotica_riesgo_invasion']) + ' ' + str(row['species_invasiveness']) if pd.notnull(row['especies_exotica_riesgo_invasion']) and pd.notnull(row['species_invasiveness']) else np.nan, axis=1)
#registros = pd.read_table('Cortes geográficos/V3. Ajustes invasoras 20240726/Bogota_data_2023-12-31_version202407.tsv', sep='\t', encoding = "utf8",dtype=str)   
##Dejar solo las presencias
#registros=registros[registros['occurrenceStatus']=='PRESENT']
#registros = pd.read_table('RNLP-RIIPPV/dwc_co_2023T4_RIIPPV.tsv', sep='\t', encoding = "utf8",dtype=str)   
#registros = pd.read_table('RNLP-RIIPPV/dwc_co_2023T4_RNLP.tsv', sep='\t', encoding = "utf8",dtype=str)   
registros['speciesLen']=registros.species.str.count(' ')
#registros['slug_x']='bogota-dc'
#registros=registros[registros['stateProvince']=='Santander']

registros.loc[registros.speciesLen<1.0,'species']=''
#registros = registros.rename(columns={'especies_exotica_riesgo_invasion_total': 'especies_exotica_riesgo_invasion'})

##Reemplaza los campos vacios por NaN, esto para evitar errores de conversión de datos al momento de
## hacer los calculos
registros = registros.replace(r'^\s*$', np.nan, regex=True)

#Concatenar el nivel de potencial de invasión en el campo de especies_exotica_riesgo_invasion
#registros['especies_exotica_riesgo_invasion'] = registros.apply(lambda row: str(row['especies_exotica_riesgo_invasion']) + ' ' + str(row['species_invasiveness']) if pd.notnull(row['especies_exotica_riesgo_invasion']) and pd.notnull(row['species_invasiveness']) else np.nan, axis=1)

## Cargar el archivo de grupos biologicos que relacionan la taxonomía con los grupos biológicos
grupos_biologicos = pd.read_table('Grupos_biologicos/gruposBiologicosCifrasSiB-2026.tsv', sep='\t', encoding = "utf8")
grupos_biologicos=grupos_biologicos.loc[grupos_biologicos.loc[:, 'tipo_grupo'] != '-']


##Archivo del último reporte mensual
entidades_reporte= pd.read_table('reporteMensual/datasetCO_20251231.txt', sep='\t', encoding = "utf8",usecols=['publishingOrganizationKey','Logo','URLSocio','typeOrg'])

##Cargar archivos de referencia geográfica 
##Con la información de departamentos
staProv_divipola = pd.read_table('divipola/departamento.tsv', encoding = "utf8")

estimadas_dept = pd.read_table('divipola/estimadas_departamentos_2023.csv', encoding = "utf8", sep=';')

##Con la información de municipios
staProv_divipola_m = pd.read_table('divipola/region.tsv', encoding = "utf8",usecols=['parent','slug','label'])

##Ajuste de las categorías threathStatus para asegurar integridad de las cifras MADS e IUCN en los conteos
##Esta sección solo se habilita cuando los registros no han pasado por el proceso de limpieza previo

#registros.threatStatus_UICN=registros.threatStatus_UICN + '_IUCN'
#registros.threatStatus_MADS=registros.threatStatus_MADS + '_MADS'
##----------------------------------------------------3. Tipo de ejecución--------------------------------------------------##
'''
La variable 'tipo' permite condicionar los procesos teniendo en cuenta si se van a sacar cifras departamentales o municipales y si 
el conjunto de datos contiene información de registros marítimos
Colombia con datos marinos='CCDM'
Colombia sin datos marinos='CSDM'
Departamental con datos marinos='DCDM'
Departamental sin datos marinos='DSDM'
Municipal con datos marinos='MCDM'
Municipal sin datos marinos='MSDM'
Otros='OT'
Al seleccionar alguna de las opciones sin datos marinos, las cifras se calculan en forma general, sin discriminar por los hábitat marino, 
terrestre y salobre

Para las opciones con datos marinos, se realiza el cálculo de cifras general y adicionalmente el cálculo para los hábitat marino, terrestre 
y salobre. Es importante aclarar que debido a los hábitos y distribución de las especies se pueden encontrar especies presentes en más de 
un hábitat por lo tanto, el valor general no corresponde a la suma de los valores para cada hábitat.


#tipo='DCDM'
#tipo='DSDM'
#tipo='MCDM'
#tipo='MSDM'
tipo='CCDM'
#tipo='CSDM'
 
''' 


def ejecucion_cifras (registros, tipo,region):
    if tipo=='DCDM' or tipo=='DSDM':    
        #nombre='RIIPPV_'
        nombre='Departamentales_'
        #nombre='PCV_regionales_'
        #nombre='PCV_categoria_'
    if tipo=='CCDM' or tipo=='CSDM':
        registros['slug_col']='colombia'
        nombre='Nacionales_'
        region='slug_col' 
        slug_region='colombia'
        
        ##Ejemplo para ejecución de cifras para elevaciones region andina
        #registros['slug_col']='elevacion_mas_2800'
        #nombre='elevacion_mas_2800_'
        #slug_region='elevacion_mas_2800'
        
        ##Ejemplo para ejecución de cifras para resguardo indígena
        #registros['slug_col']='resguardo-indigena-pialapi-pueblo-viejo'
        #nombre='RIPPV_'
        #slug_region='resguardo-indigena-pialapi-pueblo-viejo'
        
        ##Ejemplo para ejecución de cifras para reserva forestal
        #registros['slug_col']='reserva-forestal-la-planada'
        #nombre='RNLP_'
        #slug_region='reserva-forestal-la-planada'
        
    if tipo=='MCDM' or tipo=='MSDM':
        nombre='Municipales_' 
        #nombre='BogotaMunicipales_' 
    ##---------------------------------------------4. Cifras generales organizaciones publicadoras---------------------------------##

#    Se obtienen las cifras de especies y registros para todas las organizaciones publicadoras que se encuentran representadas en el conjunto de datos
    

    
    ##Quitar el texto: "País publicador:" del campo 'publishingCountry'
    registros=registros.replace('País publicador: ','',regex=True)
    
    ##Crear dataframes vacíos para almacenar las cifras de entidades publicadoras
    entidades_registros=pd.DataFrame()
    entidades_total=pd.DataFrame()

    
    ##Crear dataframe con los nombres, país y logo de las organizaciones publicadoras
    entidades =pd.DataFrame()
    entidades['publishingOrgKey'],entidades['organization'],entidades['publishingCountry'],entidades['logoUrl']=registros['publishingOrgKey'],registros['organization'],registros['publishingCountry'],registros['logoUrl']
    entidades=entidades.drop_duplicates()
    entidades_sp_tax=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species')
    ##Condicional para registros marinos, continentales y salobres
        
    if tipo =='CSDM' or tipo =='MSDM' or tipo =='DSDM': 
        ## Cifras registros por organización publicadora: Crear un dataframe con los datos agrupar por organización y contar los registros generales
        entidades_registros['registros']= registros.groupby([region,'organization'])['gbifID'].count()
        
        ##Crear conjunto de datos con especies y entidad publicadora general
        ##Contar número de especies para entidad publicadora general
        #entidades_sp=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species')
        entidades_sp= entidades_sp_tax.groupby([region,'organization','species'])['species'].count().to_frame(name = 'especies').reset_index()
        entidades_sp= entidades_sp.groupby([region,'organization'])['especies'].count().to_frame(name = 'especies').reset_index() 
    
        ##Unir los conteos de registros y especies por entidad general
        entidades_total=pd.merge(entidades_registros,entidades_sp, on=[region,'organization'],how='left').sort_values('organization').fillna('')
        
        ##Agregar la información del logo y seleccionar las columnas finales
        entidades_total=pd.merge(entidades_total,entidades, on=['organization'],how='left').drop_duplicates()
        aggregation_functions = {'publishingOrgKey': 'first', 'publishingCountry': 'first', 'logoUrl':'first','registros':'first','especies': 'first'}
        entidades_total = entidades_total.groupby(['organization',region]).aggregate(aggregation_functions).reset_index().fillna('')
    
        ##completar con la información del reporte mensual      
        entidades_total=pd.merge(entidades_total,entidades_reporte,left_on='publishingOrgKey',right_on='publishingOrganizationKey',how='left')
        
        ##Asignar la URL del logo proveniente del reporte
        entidades_total.loc[(entidades_total.logoUrl!=entidades_total.Logo) & (entidades_total.Logo!=np.nan),'logoUrl']=entidades_total.Logo
        entidades_total=entidades_total.drop(['Logo'], axis='columns')
        aggregation_functions = {'publishingOrgKey': 'first', 'publishingCountry': 'first', 'logoUrl':'first', 'typeOrg':'first','registros':'first','especies': 'first'}
        entidades_total = entidades_total.groupby(['organization',region]).aggregate(aggregation_functions).reset_index().fillna('')

    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        ## Cifras registros por organización publicadora: Crear un dataframe con los datos, agrupar por organización y contar los registros para cada una de las categorías (marinos, salobres y continentales)
        ##Crear conjunto de datos con registros y organización para datos marinos, salobres y continentales
        entidades_registros['registros'],entidades_registros['registrosMarinos'],entidades_registros['registrosSalobres'],entidades_registros['registrosContinentales']= registros.groupby(['organization',region])['gbifID'].count(),registros.groupby(['organization',region])['isMarine'].count(),registros.groupby(['organization',region])['isBrackish'].count(),registros.groupby(['organization',region])['isTerrestrial'].count()
        
        ## Cifras especies por organización publicadora: Crear un dataframe con los datos, agrupar por organización y especie, contar los registros por species para cada una de las categorías (marinos, salobres y continentales)
        entidades_sp= entidades_sp_tax.groupby(['organization','species',region])['species'].count().to_frame(name = 'especies').reset_index()
        entidades_sp= entidades_sp.groupby(['organization',region])['especies'].count().to_frame(name = 'especies').reset_index().drop_duplicates()  
        
        ##Crear conjunto de datos con especies y entidad publicadora para datos marinos
        ##Contar número de especies marinos para entidad publicadora 
        ent_sp_marino= entidades_sp_tax.groupby(['organization','species','isMarine',region])['species'].count().to_frame(name = 'especies').reset_index()
        ent_sp_marino= ent_sp_marino.groupby(['organization',region])['especies'].count().to_frame(name = 'especiesMarinas').reset_index().drop_duplicates()  
    
        ##Crear conjunto de datos con especies y entidad publicadora para datos salobres
        ##Contar número de especies salobres para entidad publicadora 
        ent_sp_salobre= entidades_sp_tax.groupby(['organization','species','isBrackish',region])['species'].count().to_frame(name = 'especies').reset_index()
        ent_sp_salobre= ent_sp_salobre.groupby(['organization',region])['especies'].count().to_frame(name = 'especiesSalobres').reset_index().drop_duplicates() 
    
        ##Crear conjunto de datos con especies y entidad publicadora para datos continentales
        ##Contar número de especies continentales para entidad publicadora
        ent_sp_continental= entidades_sp_tax.groupby(['organization','species','isTerrestrial',region])['species'].count().to_frame(name = 'especies').reset_index() 
        ent_sp_continental= ent_sp_continental.groupby(['organization',region])['especies'].count().to_frame(name = 'especiesContinentales').reset_index().drop_duplicates()  
     
        ##Unir todos los conjuntos de datos, Reemplazar los valores NaN y ordenar las entidades geográficas
        entidades_total=registros.groupby(['organization',region])['organization'].count().to_frame(name = 'especies').reset_index().drop('especies', axis=1)  
        #print(entidades_total.shape)
        entidades_total=pd.merge(entidades_total,entidades_sp, on=['organization',region],how='left').merge(ent_sp_salobre, on=['organization',region],how='left').merge(ent_sp_marino, on=['organization',region],how='left').merge(ent_sp_continental, on=['organization',region],how='left').merge(entidades_registros, on=['organization',region],how='left').sort_values('organization').fillna('').drop_duplicates() 
        ##Agregar la información del logo y seleccionar las columnas finales
       
        entidades_total=pd.merge(entidades_total,entidades, on=['organization'], how='left').drop_duplicates()   

        aggregation_functions = {'publishingOrgKey': 'first', 'publishingCountry': 'first', 'logoUrl':'first','registros':'first','registrosContinentales':'first','registrosSalobres':'first','registrosMarinos':'first','especies': 'first','especiesSalobres': 'first','especiesMarinas': 'first','especiesContinentales': 'first'}
        entidades_total = entidades_total.groupby(['organization',region]).aggregate(aggregation_functions).reset_index().fillna('')
    
        ##completar con la información del reporte mensual
        entidades_total=pd.merge(entidades_total,entidades_reporte,left_on='publishingOrgKey',right_on='publishingOrganizationKey',how='left')
        ##Asignar la URL del logo proveniente del reporte
        entidades_total.loc[(entidades_total.logoUrl!=entidades_total.Logo) & (entidades_total.Logo!=np.nan),'logoUrl']=entidades_total.Logo
        entidades_total=entidades_total.drop(['Logo'], axis='columns')
        aggregation_functions = {'publishingOrgKey': 'first', 'publishingCountry': 'first', 'logoUrl':'first', 'typeOrg':'first','registros':'first','registrosSalobres':'first','registrosMarinos':'first','registrosContinentales':'first','especies': 'first','especiesSalobres': 'first','especiesMarinas': 'first','especiesContinentales': 'first'}
        entidades_total = entidades_total.groupby(['organization',region]).aggregate(aggregation_functions).reset_index().fillna('')
        
        del ent_sp_marino
        del ent_sp_salobre
        del ent_sp_continental
    
    del entidades_registros
    del entidades_sp   
    del entidades
    del entidades_sp_tax
    
    ##Quitar los .0
    entidades_total=entidades_total.astype(str)
    entidades_total=entidades_total.replace(to_replace='\.0+$',value="",regex=True)
    
    ##Reemplazar los nombres y la url del logo para las entidades que lo requieran
    entidades_total.loc[entidades_total.organization=='Asociación de Becarios del Casanare','organization']='ABC Colombia Somos Territorio'
    entidades_total.loc[entidades_total.organization=='iNaturalist.org','organization']='Naturalista Colombia'
    entidades_total.loc[entidades_total.organization=='Naturalista Colombia','publishingCountry']='CO'
    entidades_total.loc[entidades_total.organization=='Cornell Lab of Ornithology','publishingCountry']='CO'
    entidades_total.loc[entidades_total.organization=='Naturalista Colombia','logoUrl']='https://statics.sibcolombia.net/sib-resources/images/logos-socios/portal-sib/logo-naturalistaco.png'
    entidades_total.loc[entidades_total.publishingCountry=='CO','tipoPublicador']='Nacional'
    entidades_total.loc[entidades_total.publishingCountry!='CO','tipoPublicador']='Internacional'
    entidades_total.loc[entidades_total.organization=='Naturalista Colombia','typeOrg']='Redes/Iniciativas'
    entidades_total.loc[entidades_total.organization=='Cornell Lab of Ornithology','typeOrg']='Redes/Iniciativas'
    entidades_total.loc[entidades_total.organization=='Cornell Lab of Ornithology','organization']='eBird Colombia'
    entidades_total.loc[entidades_total.organization=='eBird Colombia','logoUrl']='https://statics.sibcolombia.net/sib-resources/images/logos-socios/500px/ebird.jpg'
    entidades_total.loc[entidades_total.tipoPublicador=='Internacional','logoUrl']='https://statics.sibcolombia.net/sib-resources/images/santander/world.png'
    entidades_total.loc[entidades_total.publishingOrgKey=='827fad55-4521-496e-949c-28e3b0428765','typeOrg']='ONG'
    entidades_total.loc[entidades_total.publishingOrgKey=='c803f6f5-2c6a-4b41-8c15-768d48ef1c8c','typeOrg']='ONG'
    entidades_total.loc[entidades_total.publishingOrgKey=='c803f6f5-2c6a-4b41-8c15-768d48ef1c8c','logoUrl']='https://raw.githubusercontent.com/SIB-Colombia/logos/main/socio-SiB-abc.png'
    entidades_total.loc[entidades_total.publishingOrgKey=='827fad55-4521-496e-949c-28e3b0428765','logoUrl']='https://raw.githubusercontent.com/SIB-Colombia/logos/main/socio-SiB-cunaguaro.png'
    entidades_total.loc[entidades_total.publishingOrgKey=='e62a5313-e771-4c81-b6d1-cba6e4085635','logoUrl']='https://raw.githubusercontent.com/SIB-Colombia/logos/main/socio-SiB-aures.png'
    entidades_total.loc[entidades_total.publishingOrgKey=='112087f6-a6c0-4cee-8441-387f900d34f9','logoUrl']='https://raw.githubusercontent.com/SIB-Colombia/logos/main/socio-SiB-udes.png'
    entidades_total['URLSocio']='https://biodiversidad.co/data/?publishingOrg='+entidades_total['publishingOrgKey']
    
    entidades_total.loc[entidades_total.typeOrg=='Centros/Institutos de Investigación','typeOrg']='Centros de investigación'
    entidades_total.loc[entidades_total.typeOrg=='Autoridades Ambientales','typeOrg']='Autoridades ambientales'
    entidades_total.loc[entidades_total.typeOrg=='Redes/Iniciativas','typeOrg']='Redes e iniciativas'
    entidades_total.loc[entidades_total.typeOrg=='Entidades Administrativas Territoriales','typeOrg']='Entidades territoriales'
    entidades_total.loc[entidades_total.typeOrg=='Internacional','typeOrg']='Internacional'
    
    entidades_total.loc[entidades_total.tipoPublicador=='Internacional','typeOrg']='Internacional'
    ##Exportar los datos a archivos tsv y xlsx
    
    if tipo =='CCDM' or tipo =='MCDM' or tipo =='DCDM':
        #entidades_total['slug_region']='colombia'
        publicadores=entidades_total
        publicadores.columns=['label','slug_region','slug','pais_publicacion','url_logo','tipo_organizacion','registros','registros_salobres','registros_marinos','registros_continentales','especies','especies_salobres','especies_marinas','especies_continentales','tipo_publicador','url_socio']
        publicadores=publicadores[['slug','label','pais_publicacion','tipo_organizacion','tipo_publicador','url_logo','url_socio','especies','registros']]    
        
        entidades_total.columns=['label','slug_region','slug_publicador','pais_publicacion','url_logo','tipo_organizacion','registros','registros_salobres','registros_marinos','registros_continentales','especies','especies_salobres','especies_marinas','especies_continentales','tipo_publicador','url_socio']
        entidades_total=entidades_total[['slug_region','slug_publicador','registros','registros_continentales','registros_marinos','registros_salobres','especies','especies_continentales','especies_marinas','especies_salobres']]  
        
        publicadores = publicadores.drop_duplicates()  
        publicadores.to_csv(nombre+'publicador.tsv',sep='\t', index=False )
        publicadores.to_excel(nombre+'publicador.xlsx', sheet_name='cifrasEntidades', index=False )
    
    if tipo =='CSDM' or tipo =='MSDM' or tipo =='DSDM':
        #entidades_total['slug_region']='colombia'
        publicadores=entidades_total
    
        publicadores.columns=['label','slug_region','slug','pais_publicacion','url_logo','tipo_organizacion','registros','especies','tipo_publicador','url_socio']
        publicadores=entidades_total[['slug','label','pais_publicacion','tipo_organizacion','tipo_publicador','url_logo','url_socio','especies','registros']]
        
        entidades_total.columns=['label','slug_region','slug_publicador','pais_publicacion','url_logo','tipo_organizacion','registros','especies','tipo_publicador','url_socio']
        entidades_total=entidades_total[['slug_region','slug_publicador','registros','especies']]
        
        publicadores = publicadores.drop_duplicates()    
        publicadores.to_csv(nombre+'publicador.tsv',sep='\t', index=False )
        publicadores.to_excel(nombre+'publicador.xlsx', sheet_name='cifrasEntidades', index=False )
    
    
    entidades_total.to_csv(nombre+'region_publicador.tsv',sep='\t', index=False )
    entidades_total.to_excel(nombre+'region_publicador.xlsx', sheet_name='cifrasEntidades', index=False )
    del entidades_total
    del publicadores
    ##----------------------------------------------------5. Obtener tabla de especie--------------------------------------------------##
    
    if tipo =='CSDM' or tipo =='CCDM':
        ##Solo se habilita para obtener la lista de especies total para el país y si se cuenta con un corte de datos actualizado
        ##Obtener los nombres de las especies con su taxonomía superior
        #especies_colombia=registros.groupby(['species'])['kingdom', 'phylum', 'class', 'order', 'family', 'genus','flagTAXO'].first().reset_index()
        especies_colombia=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
#        especies_colombia=especies_colombia.groupby(['species'])['kingdom', 'phylum', 'class', 'order', 'family', 'genus'].first().reset_index()
        especies_colombia = (especies_colombia.groupby('species').agg({'kingdom': 'first','phylum': 'first','class': 'first','order': 'first','family': 'first','genus': 'first'}).reset_index())

        ##Crear la columna de slug
        especies_colombia['slug']=especies_colombia['species'].str.lower().replace(to_replace=' ',value="-",regex=True)
        
        ##Reordenar las columnas
        #especies_colombia=especies_colombia[['slug','species','kingdom', 'phylum', 'class', 'order', 'family', 'genus','flagTAXO']]
        especies_colombia=especies_colombia[['slug','species','kingdom', 'phylum', 'class', 'order', 'family', 'genus']]
        ##crear archivo tsv 
        especies_colombia.to_csv(nombre+'especie.tsv',sep='\t', index=False)     
        
        ##crear archivo excel
        especies_colombia.to_excel(nombre+'especie.xlsx', sheet_name='Lista especies', index=False)    
        del especies_colombia
    ##----------------------------------------------------6. Obtener tabla de especie_meta--------------------------------------------------##
    if tipo =='CSDM' or tipo =='CCDM':
        ##Obtener los nombres de las especies con su taxonomía superior
        #especies_colombia=registros.groupby(['species'])['kingdom', 'phylum', 'class', 'order', 'family', 'genus','flagTAXO'].first().reset_index()
        especies_colombia=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
#        especies_colombia=especies_colombia.groupby(['species'])['kingdom', 'phylum', 'class', 'order', 'family', 'genus'].first().reset_index()
        especies_colombia = (especies_colombia.groupby('species').agg({'kingdom': 'first','phylum': 'first','class': 'first','order': 'first','family': 'first','genus': 'first'}).reset_index())
        ##Crear la columna de slug
        especies_colombia['slug']=especies_colombia['species'].str.lower().replace(to_replace=' ',value="-",regex=True)
        
        ##Reordenar las columnas
        #especies_colombia=especies_colombia[['slug','flagTAXO']]
        especies_colombia=especies_colombia[['slug']]
        especies_colombia['vernacular_name_es']=''
        especies_colombia['url_gbif']=''
        especies_colombia['url_cbc']=''
        
        ##crear archivo tsv 
        especies_colombia.to_csv(nombre+'especie_meta.tsv',sep='\t', index=False)     
        
        ##crear archivo excel
        especies_colombia.to_excel(nombre+'especie_meta.xlsx', sheet_name='Lista especies meta', index=False)       
        
        del especies_colombia
    ##-------------------------------------------------7. Calcular especie_grupo_biologico y especie_grupo_interes_conservacion---------------------------------------------------##
    ##Crear los dataFrames que almacenaran la información separada de grupos 
    ##biologicos y grupos de interes

    if tipo =='CSDM' or tipo =='CCDM':
        ##Crear los diccionarios que contienen la información de cada ciclo por 
        ##categoría taxonomica
        dic_kingdom,dic_phylum, dic_class, dic_order,dic_family,dic_genus,dic_species={},{},{},{},{},{},{}
        
        ##Obtener el listado de espcies unicas con su taxonomía superior
        especies_colombiagb=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
#        especies_colombiagb=especies_colombiagb.groupby(['species'])['kingdom', 'phylum', 'class', 'order', 'family', 'genus'].first().reset_index()
        especies_colombiagb = (especies_colombiagb.groupby('species').agg({'kingdom': 'first','phylum': 'first','class': 'first','order': 'first','family': 'first','genus': 'first'}).reset_index())

        ##Obtener los grupos biologicos que aplican en cada categoría taxonomíca
        reino=grupos_biologicos[grupos_biologicos['taxonRank']=='kingdom']
        filo=grupos_biologicos[grupos_biologicos['taxonRank']=='phylum']
        clase=grupos_biologicos[grupos_biologicos['taxonRank']=='class']
        orden=grupos_biologicos[grupos_biologicos['taxonRank']=='order']
        familia=grupos_biologicos[grupos_biologicos['taxonRank']=='family']
        genero=grupos_biologicos[grupos_biologicos['taxonRank']=='genus']
        especie=grupos_biologicos[grupos_biologicos['taxonRank']=='species']
        
        
        #Obtener los id de cada grupo primero para grupos biologicos 
        ##y despues para grupos de interes
        gb=list(grupos_biologicos['grupo_id'][grupos_biologicos['tipo_grupo']=='biologico'])
        gi=list(grupos_biologicos['grupo_id'][grupos_biologicos['tipo_grupo']=='interes'])
        
        
        ##Se recorre el listado de especies unica y los diferentes niveles taxonomicos
        ##en el caso de que la especie o la taxonomia superior coincida con algún elemento
        ##del dataFrame de cada categoría se agrega al diccionario correspondiente
        for i in especies_colombiagb.index:
            for ii in reino.index:
                if especies_colombiagb['kingdom'][i]==reino['grupoTax'][ii]:  
                    dic_kingdom[especies_colombiagb['species'][i]+'|'+reino['grupo_id'][ii]]=reino['grupo_id'][ii]
                    
            for ii in filo.index:
                if especies_colombiagb['phylum'][i]==filo['grupoTax'][ii]:         
                    dic_phylum[especies_colombiagb['species'][i]+'|'+filo['grupo_id'][ii]]=filo['grupo_id'][ii]
        
            for ii in clase.index:
                if especies_colombiagb['class'][i]==clase['grupoTax'][ii]:        
                    dic_class[especies_colombiagb['species'][i]+'|'+clase['grupo_id'][ii]]=clase['grupo_id'][ii]
            
            for ii in orden.index:        
                if especies_colombiagb['order'][i]==orden['grupoTax'][ii]:        
                    dic_order[especies_colombiagb['species'][i]+'|'+orden['grupo_id'][ii] ]=orden['grupo_id'][ii] 
                    
            for ii in familia.index:
                if especies_colombiagb['family'][i]==familia['grupoTax'][ii]:        
                    dic_family[especies_colombiagb['species'][i]+'|'+familia['grupo_id'][ii] ]=familia['grupo_id'][ii] 
                    
            for ii in genero.index:
                if especies_colombiagb['genus'][i]==genero['grupoTax'][ii]:        
                    dic_genus[especies_colombiagb['species'][i]+'|'+genero['grupo_id'][ii]]=genero['grupo_id'][ii] 
            
            for ii in especie.index:
                if especies_colombiagb['species'][i]==especie['grupoTax'][ii]: 
                    dic_species[especies_colombiagb['species'][i]+'|'+especie['grupo_id'][ii]]=especie['grupo_id'][ii] 
                    #dic_species.setdefault(especies_colombiagb['species'][i], []).append(especie['grupo_id'][ii])
        
        
        ##Cada diccionario se convierte en un dataFrame, se ajustan filas y columnas
        ##y se unen en un dataFrame final
        df_kingdom = pd.DataFrame(dic_kingdom, index=['slug_grupo'])
        df_kingdom=(df_kingdom.T)
        df_kingdom = df_kingdom.rename_axis('slug_especie').reset_index()
        if df_kingdom.slug_especie.any():
            df_kingdom[['slug_especie','slug_grupo']]=df_kingdom.slug_especie.str.split('|',expand=True)
        
        df_phylum = pd.DataFrame(dic_phylum, index=['slug_grupo'])
        df_phylum=(df_phylum.T)
        df_phylum = df_phylum.rename_axis('slug_especie').reset_index()
        if df_phylum.slug_especie.any():
            df_phylum[['slug_especie','slug_grupo']]=df_phylum.slug_especie.str.split('|',expand=True)
        
        df_class = pd.DataFrame(dic_class, index=['slug_grupo'])
        df_class=(df_class.T)
        df_class = df_class.rename_axis('slug_especie').reset_index()
        if df_class.slug_especie.any():
            df_class[['slug_especie','slug_grupo']]=df_class.slug_especie.str.split('|',expand=True)
        
        df_order = pd.DataFrame(dic_order, index=['slug_grupo'])
        df_order=(df_order.T)
        df_order = df_order.rename_axis('slug_especie').reset_index()
        if df_order.slug_especie.any():
            df_order[['slug_especie','slug_grupo']]=df_order.slug_especie.str.split('|',expand=True)
        
        df_family = pd.DataFrame(dic_family, index=['slug_grupo'])
        df_family=(df_family.T)
        df_family = df_family.rename_axis('slug_especie').reset_index()
        if df_family.slug_especie.any():
            df_family[['slug_especie','slug_grupo']]=df_family.slug_especie.str.split('|',expand=True)
        
        df_genus = pd.DataFrame(dic_genus, index=['slug_grupo'])
        df_genus=(df_genus.T)
        df_genus = df_genus.rename_axis('slug_especie').reset_index()
        if df_genus.slug_especie.any():
            df_genus[['slug_especie','slug_grupo']]=df_genus.slug_especie.str.split('|',expand=True)
        
        df_species = pd.DataFrame(dic_species, index=['slug_grupo'])
        df_species=(df_species.T)
        df_species = df_species.rename_axis('slug_especie').reset_index()
        if df_species.slug_especie.any():
            df_species[['slug_especie','slug_grupo']]=df_species.slug_especie.str.split('|',expand=True)
        
        del dic_kingdom,dic_phylum, dic_class, dic_order,dic_family,dic_genus,dic_species
        
        df_final=pd.concat([df_kingdom,df_phylum,df_class,df_order,df_family,df_genus,df_species]) 
        
        ##Se aplica  un consulta en el dataFrame final y en el caso de que el grupo 
        #se encuentre en la lista de grupos biologicos o grupos de interes se mantiene 
        ##en el respectivo dataFrame
        
        
        df_final['slug_especie']=df_final['slug_especie'].str.lower().replace(to_replace=' ',value="-",regex=True)
        df_final.loc[(df_final['slug_grupo'].isin(gb)), 'tipo'] = 'biologico'
        df_final.loc[(df_final['slug_grupo'].isin(gi)), 'tipo'] = 'interes'
        
        del df_kingdom, df_phylum, df_class, df_order,df_family,df_genus,df_species
        ##crear archivo csv 
        df_final.to_csv(nombre+'especie_grupo.tsv',sep='\t', index=False )     
        
        ##crear archivo excel y la hoja Cifras totales
        df_final.to_excel(nombre+'especie_grupo.xlsx', sheet_name='Lista especies_grupoBiologico', index=False )         
        del df_final
    ##--------------------------------------------8. Calcular número de registros por especie en la región ---------------------------------------------##
    
    if tipo=='DCDM' or tipo=='DSDM' or tipo=='MCDM' or tipo=='MSDM':
        especies_region=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
        especies_region=especies_region.groupby(['species',region])['species'].count().to_frame(name = 'registros').reset_index()
        ##Crear la columna de slug
        especies_region['slug_especie']=especies_region['species'].str.lower().replace(to_replace=' ',value="-",regex=True)
        especies_region.columns=['species','slug_region','registros','slug_especie']
        especies_region=especies_region[['slug_region','slug_especie','registros']]
    
    if tipo=='CCDM' or tipo=='CSDM':
        especies_region=registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
        especies_region=especies_region.groupby(['species'])['species'].count().to_frame(name = 'registros').reset_index()
    
        ##Crear la columna de slug
        especies_region['slug_especie']=especies_region['species'].str.lower().replace(to_replace=' ',value="-",regex=True)
        especies_region['slug_region']=slug_region
        especies_region.columns=['species','registros','slug_especie','slug_region']
        especies_region=especies_region[['slug_region','slug_especie','registros']]
        
    
    ##crear archivo csv 
    especies_region.to_csv(nombre+'especie_region.tsv',sep='\t', index=False)     
    ##crear archivo excel y la hoja Cifras totales
    especies_region.to_excel(nombre+'especie_region.xlsx', sheet_name='Lista especies', index=False) 
    
    del especies_region
    
    ##--------------------------------------------9. Especies con tematica por región ---------------------------------------------##
    '''
    El loop permite iterar en el conjunto de datos, realizando diferentes procesos y creación de variables, teniendo en cuenta 
    las categorias tematicas y los grupos taxonómicos 
    
    Dentro de los nombres se encuentran las siguientes convenciones:
    rb= Registros biologicos
    sp= especies
    geo= geografía
    '''
    
    sp_tematica_region=pd.DataFrame()
    
    ## Crear listas con las categorías temáticas sobre las cuales se corre el loop
    tematica =['threatStatus_UICN','especies_invasoras','appendixCITES','threatStatus_MADS','especies_exoticas','especies_exotica_riesgo_invasion','especies_trasplantadas','endemic','migratory']
    registros_tax=registros[(registros['flagTAXO']!='Ausente en lista taxonómica')]#.drop_duplicates('species').sort_values(by=['species'])
       
    for t in tematica:                       
        ##Se crea una variable que almacena las especies y la temática de interés
        if tipo=='DCDM' or tipo=='DSDM' or tipo=='MCDM' or tipo=='MSDM':
            rb_numero_categoria = registros_tax.groupby(['species',t,region])[t].count().to_frame(name = 'registros' ).reset_index()
        if tipo=='CCDM' or tipo=='CSDM':
            rb_numero_categoria = registros_tax.groupby(['species',t])[t].count().to_frame(name = 'registros' ).reset_index()
            rb_numero_categoria['slug_col']=slug_region
            
        rb_numero_categoria['thematic']=t
        rb_numero_categoria = rb_numero_categoria[rb_numero_categoria['species'].notna()]  
        rb_numero_categoria=rb_numero_categoria.rename(columns={t:'category'})
        sp_tematica_region=pd.concat([sp_tematica_region,rb_numero_categoria])    
    
    del rb_numero_categoria
    ##Se eliminan duplicados y se terminan de ajustar los nombres de todo el conjunto de datos
    sp_tematica_region = sp_tematica_region.drop_duplicates()
    sp_tematica_region.category=sp_tematica_region.category.replace('Exótica con potencial de invasión Alto Riesgo','exotica-riesgo-invasion-alto',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('Exótica con potencial de invasión Bajo Riesgo','exotica-riesgo-invasion-bajo',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('Exótica con potencial de invasión Riesgo Moderado','exotica-riesgo-invasion-moderado',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('Exótica con potencial de invasión Riesgo Moderado/ Alto','exotica-riesgo-invasion-moderado-alto',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('LC_IUCN','amenazadas-global-lc',regex=True)         
    sp_tematica_region.category=sp_tematica_region.category.replace('NT_IUCN','amenazadas-global-nt',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('VU_IUCN','amenazadas-global-vu',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('EN_IUCN','amenazadas-global-en',regex=True)           
    sp_tematica_region.category=sp_tematica_region.category.replace('CR_IUCN','amenazadas-global-cr',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('DD_IUCN','amenazadas-global-dd',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('LR/lc_IUCN','amenazadas-global-lr-lc',regex=True)           
    sp_tematica_region.category=sp_tematica_region.category.replace('LR/nt_IUCN','amenazadas-global-lr-nt',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('EW_IUCN','amenazadas-global-ew',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('EX_IUCN','amenazadas-global-ex',regex=True)           
    sp_tematica_region.category=sp_tematica_region.category.replace('NE_IUCN','amenazadas-global-ne',regex=True) 
    sp_tematica_region.category=sp_tematica_region.category.replace('LR/cd_IUCN','amenazadas-global-lr-cd',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('Invasora','invasoras',regex=True)  
    sp_tematica_region.category=sp_tematica_region.category.replace('I/II','cites-i-ii',regex=True)   
    sp_tematica_region.category=sp_tematica_region.category.replace('III','cites-iii',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('II','cites-ii',regex=True)            
    sp_tematica_region.category=sp_tematica_region.category.replace('I','cites-i',regex=True)         
    sp_tematica_region.category=sp_tematica_region.category.replace('VU_MADS','amenazadas-nacional-vu',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('EN_MADS','amenazadas-nacional-en',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('CR_MADS','amenazadas-nacional-cr',regex=True)           
    sp_tematica_region.category=sp_tematica_region.category.replace('Exótica','exoticas',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('Endémica','endemicas',regex=True)           
    sp_tematica_region.category=sp_tematica_region.category.replace('Migratorio','migratorias',regex=True)    
    sp_tematica_region.category=sp_tematica_region.category.replace('Errática','erraticas',regex=True)
    sp_tematica_region.category=sp_tematica_region.category.replace('Residente','residente',regex=True)   
    sp_tematica_region.category=sp_tematica_region.category.replace('Trasplantada','trasplantadas',regex=True)          
    
    
    
    sp_tematica_region['slug_especie']=sp_tematica_region['species'].str.lower().replace(to_replace=' ',value="-",regex=True)
    
    if tipo=='MCDM' or tipo=='MSDM':
        sp_tematica_region=sp_tematica_region[['slug_especie','slug_y','category']]
        sp_tematica_region.columns=['slug_especie','slug_region','slug_tematica']
    
    if tipo=='DCDM' or tipo=='DSDM' or tipo=='CCDM' or tipo=='CSDM':
        sp_tematica_region=sp_tematica_region[['slug_especie',region,'category']]
        sp_tematica_region.columns=['slug_especie','slug_region','slug_tematica']
    
    ##crear archivo csv 
    sp_tematica_region.to_csv(nombre+'especie_tematica.tsv',sep='\t', index=False)     
    ##crear archivo excel y la hoja Cifras totales
    sp_tematica_region.to_excel(nombre +'especie_tematica.xlsx', sheet_name='Lista especies', index=False)         
    
    del sp_tematica_region
    ##----------------------------------------------------10. Cifras totales -----------------------------------------------------------##
    '''
    En esta sección se calculan las cifras totales para el departamento o el país sin tener en cuenta temáticas de interés, publicadores
    o grupos biológicos
    
    '''
    ##Crear un dataframe vacío
    total_cifras=pd.DataFrame(index=[0])
    #variable_conteos_marinos=pd.DataFrame()
    
    ##Crear variable para los conteos por especie general
    variable_conteos = registros[(registros['species'].notna()) & (registros['flagTAXO']!='Ausente en lista taxonómica')].drop_duplicates('species').sort_values(by=['species']) 
    
    
    ##Crear la columna registros y especies general contando el campo gbifID
    total_cifras['registros'],total_cifras['especies']=registros['gbifID'].count(),variable_conteos['gbifID'].count()
    
    ##Condicional para registros marinos, continentales y salobres
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        ##Crear un subconjunto de datos para los registros marinos, continentales y salobres
        registros_marinos=registros[registros['isMarine']=='Marine']
        registros_continentales=registros[registros['isTerrestrial']=='Terrestrial']
        registros_salobres=registros[registros['isBrackish']=='Brackish']
    
        ##Crear variables para los conteos por especie de las categorias marinos, continentales y salobres
        variable_conteos_marinos = registros_marinos[registros_marinos['species'].notna() & (registros['flagTAXO']!='Ausente en lista taxonómica')].drop_duplicates('species').sort_values(by=['species']) #se puede dejar en una sola variable
        variable_conteos_continentales = registros_continentales[registros_continentales['species'].notna() & (registros['flagTAXO']!='Ausente en lista taxonómica')].drop_duplicates('species').sort_values(by=['species']) #se puede dejar en una sola variable
        variable_conteos_salobres = registros_salobres[registros_salobres['species'].notna() & (registros['flagTAXO']!='Ausente en lista taxonómica')].drop_duplicates('species').sort_values(by=['species']) #se puede dejar en una sola variable
    
        ##Crear las columnas registros marinos y especies marinas
        total_cifras['registrosMarinos'],total_cifras['especiesMarinas']=registros['isMarine'].count(),variable_conteos_marinos['isMarine'].count()
        
        ##Crear la columna registros salobres y especies salobres
        total_cifras['registrosSalobres'],total_cifras['especiesSalobres']=registros['isBrackish'].count(),variable_conteos_salobres['isBrackish'].count()
    
        ##Crear las columnas registros continentales y especies continentales
        total_cifras['registrosContinentales'],total_cifras['especiesContinentales']=registros['isTerrestrial'].count(),variable_conteos_continentales['isTerrestrial'].count()
        total_cifras.columns=['registros','especies','registros_marinos','especies_marinas','registros_salobres', 'especies_salobres', 'registros_continentales', 'especies_continentales']
        total_cifras=total_cifras[['registros','registros_continentales','registros_marinos','registros_salobres','especies','especies_continentales', 'especies_marinas', 'especies_salobres']]

        del variable_conteos_marinos
        del variable_conteos_continentales
        del variable_conteos_salobres
    ##crear archivo tsv 
    total_cifras.to_csv(nombre+'cifrasTotales.tsv',sep='\t', index=False ) 
    
    ##crear archivo excel y la hoja Cifras totales
    total_cifras.to_excel(nombre +'cifrasTotales.xlsx', sheet_name='Cifras totales', index=False)
    
    del total_cifras


    ##---------------------------------------------11. Cifras generales geográficas --------------------------------------------------##
    '''
    Dentro de esta sección se define el alcance geográfico del conjunto de datos. Se selecciona si se va a trabajar a nivel nacional o
    departamental. En caso de que se trabaje a nivel nacional, las cifras geográficas harán referencia a los departamentos. Para el nivel 
    departamental las cifras se obtendrán para cada uno de los municipios encontrados dentro del departamento de interés
    Dentro de los nombres se encuentran las siguientes convenciones:
    rb= Registros biologicos
    sp= especies
    geo= geografía
    '''
    
    ##Crear dataframes vacíos para almacenar las cifras geográficas de especies y registros
    geo_rb_total=pd.DataFrame()
    geo_sp_total=pd.DataFrame()
    
    ##Cifras registros por entidad geográfica: Crear un dataframe con los datos, agrupar los datos por entidad geográficas y contar los registros generales
    ##Crear columna registros
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM' or tipo =='CCDM' or tipo =='CSDM':
        geo_rb_total['registros']= registros.groupby(region)['gbifID'].count()
    
        ##Cifras especies por entidad geográfica: Crear un dataframe con los datos, agrupar los datos por entidad geografica y contar los registros por species
        ##Crear un dataframe con los datos. Agrupar por entidad geografica y contar los registros por species
        geo_sp_total= registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,'species'])['species'].count().to_frame(name = 'registros').reset_index()
        geo_sp_total= geo_sp_total.groupby([region])['species'].count().to_frame(name = 'especies').reset_index().sort_values(region)#.fillna('-')
    
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        
        ##Cifras registros por entidad geográfica: Crear un dataframe con los datos, agrupar los datos por entidad geográfica y contar los registros para cada una de las categorías (marinos, salobres y continentales)
        ##Crear columna registros para datos marinos
        geo_rb_total['registrosMarinos']= registros.groupby(region)['isMarine'].count()
        ##Crear columna registros para datos salobres
        geo_rb_total['registrosSalobres']= registros.groupby(region)['isBrackish'].count()
        ##Crear columna registros para datos continentales
        geo_rb_total['registrosContinentales']= registros.groupby(region)['isTerrestrial'].count()
    
        ##Cifras especies por entidad geográfica: Crear un dataframe con los datos, agrupar los datos por entidad geográfica y contar los registros por species para cada una de las categorías (marinos, salobres y continentales)
        ##Crear conjunto de datos con especies y geografía para datos marinos y Contar número de especies marinos para entidad geográfica 
        geo_sp_marino=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,'species','isMarine'])['isMarine'].count().to_frame(name = 'registros').reset_index()
        geo_sp_marino=geo_sp_marino.groupby(region)['registros'].count().to_frame(name = 'especiesMarinas').reset_index()
        
        ##Crear conjunto de datos con especies y geografía para datos salobres y Contar número de especies salobres para entidad geográfica 
        geo_sp_salobre=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,'species','isBrackish'])['isBrackish'].count().to_frame(name = 'registros').reset_index()
        geo_sp_salobre=geo_sp_salobre.groupby(region)['registros'].count().to_frame(name = 'especiesSalobres').reset_index()
    
        ##Crear conjunto de datos con especies y geografía para datos continentales y Contar número de especies continentales para entidad geográfica 
        ##Contar número de especies salobres para entidad geográfica 
        geo_sp_continental=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,'species','isTerrestrial'])['isTerrestrial'].count().to_frame(name = 'registros').reset_index()
        geo_sp_continental=geo_sp_continental.groupby(region)['registros'].count().to_frame(name = 'especiesContinentales').reset_index()
    
        ##Unir todos los conjuntos de datos, Reemplazar los valores NaN y ordenar las entidades geográficas
        geo_sp_total=pd.merge(geo_sp_total,geo_sp_salobre, on=[region],how='left').merge(geo_sp_marino, on=[region],how='left').merge(geo_sp_continental, on=[region],how='left').sort_values(region)#.fillna('-')
        
    ##-------------------------------------------------12. Loop calculo cifras ---------------------------------------------------##
    '''
    El loop permite iterar en el conjunto de datos, realizando diferentes procesos y creación de variables, teniendo en cuenta 
    las categorias tematicas y los grupos taxonómicos 
    
    Durante el loop se realiza el conteo de los registros biologicos y el número de especies para las diferenes categorias. Dentro de 
    los nombres se encuentran las siguientes convenciones:
    rb= Registros biologicos
    sp= especies
    geo= geografía
    '''
    
    ## Crear listas con las categorías tematicas sobre las cuales se corre el loop
    tematica =['threatStatus_UICN','especies_invasoras','appendixCITES','threatStatus_MADS','especies_exoticas','especies_exotica_riesgo_invasion','especies_trasplantadas','endemic','migratory']
    
    ## Define una lista con todos las categorias taxonomicas
    taxon = ['kingdom','phylum','class','order','family','genus','species'] 
    
    ## Creación de dataframes vacíos para guardar las cifras generadas en el loop
    ## Dataframes para registros
    rb_taxon_total=pd.DataFrame()
    rb_tematica_total=pd.DataFrame()
    rb_taxon_tematica_total=pd.DataFrame()
    rb_taxon_categoria_total=pd.DataFrame()
    
    ## Dataframes para especies
    sp_taxon_total=pd.DataFrame()
    sp_tematica_total=pd.DataFrame()
    sp_taxon_tematica_total=pd.DataFrame()
    sp_taxon_categoria_total=pd.DataFrame()
    
    ## Dataframes geográficos
    geo_tematica_rb_total=pd.DataFrame() 
    geo_tematica_sp_total=pd.DataFrame()
    geo_categoria_rb_total=pd.DataFrame()
    geo_categoria_sp_total=pd.DataFrame()
    
    
    ## La primera parte del loop recorre las categorias temáticas 
    for i in tematica:
    
        if tipo =='CSDM' or tipo =='CCDM':    
            ## Cifras registros: Agrupar por categorías temáticas y contar por gbifID generales
            rb_numero = registros.groupby(i)['gbifID'].count().to_frame(name = 'registros' ).reset_index()
            ##Asignar al campo 'thematic' el valor que tiene i 
            rb_numero['thematic']=i
            rb_numero=rb_numero.rename(columns={i:'category'})
    
        if tipo =='CCDM':
            ## Cifras registros marinos: Agrupar por categorías temáticas y contar por gbifID con categoría marinos
            rb_numero_marino = registros_marinos.groupby([i,'isMarine'])['gbifID'].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
            rb_numero_marino['thematic']=i
            rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
            ## Cifras registros continentales: Agrupar por categorías temáticas y contar por gbifID con categoría continental
            rb_numero_continental = registros_continentales.groupby([i,'isTerrestrial'])['gbifID'].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            rb_numero_continental['thematic']=i
            rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})
    
            ## Cifras registros salobres: Agrupar por categorías temáticas y contar por gbifID con categoría salobre    
            rb_num_salobre = registros_salobres.groupby([i,'isBrackish'])['gbifID'].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            rb_num_salobre['thematic']=i
            rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})
            
   
            ##Agrupar las cifras marinas, continentales y salobres
            rb_tematica=pd.merge(rb_numero,rb_numero_continental, on=['category','thematic'],how='left').merge(rb_numero_marino, on=['category','thematic'],how='left').merge(rb_num_salobre, on=['category','thematic'],how='left')
            ##Almacenar la información de cada recorrido del loop
            rb_tematica_total=pd.concat([rb_tematica_total,rb_tematica])
        
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
            ## Cifras registros: Agrupar por categorías temáticas y contar por gbifID generales
            rb_numero = registros.groupby([i,region])['gbifID'].count().to_frame(name = 'registros' ).reset_index()
            ##Asignar al campo 'thematic' el valor que tiene i 
            rb_numero['thematic']=i
            rb_numero=rb_numero.rename(columns={i:'category'})
        
            ##Condicional para registros marinos, continentales y salobres
        if tipo =='MCDM' or tipo =='DCDM':
            ## Cifras registros marinos: Agrupar por categorías temáticas y contar por gbifID con categoría marinos
            rb_numero_marino = registros_marinos.groupby([i,'isMarine',region])['gbifID'].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
            rb_numero_marino['thematic']=i
            rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
            ## Cifras registros continentales: Agrupar por categorías temáticas y contar por gbifID con categoría continental
            rb_numero_continental = registros_continentales.groupby([i,'isTerrestrial',region])['gbifID'].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            rb_numero_continental['thematic']=i
            rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})
    
            ## Cifras registros salobres: Agrupar por categorías temáticas y contar por gbifID con categoría salobre    
            rb_num_salobre = registros_salobres.groupby([i,'isBrackish',region])['gbifID'].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            rb_num_salobre['thematic']=i
            rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})
    
            ##Agrupar las cifras marinas, continentales y salobres
            rb_tematica=pd.merge(rb_numero,rb_numero_continental, on=['category','thematic',region],how='left').merge(rb_numero_marino, on=['category','thematic',region],how='left').merge(rb_num_salobre, on=['category','thematic',region],how='left')
            ##Almacenar la información de cada recorrido del loop
            rb_tematica_total=pd.concat([rb_tematica_total,rb_tematica])   
        
        
        if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
            ##Almacenar la información de cada recorrido del loop
            rb_tematica_total=pd.concat([rb_tematica_total,rb_numero])
    
        if tipo =='CSDM' or tipo =='CCDM': 
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID generales
            spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([i,'species'])##
            sp_numero = spp_numero.groupby([i])['species'].count().to_frame(name = 'especies' ).reset_index()
            sp_numero['thematic']=i
            sp_numero=sp_numero.rename(columns={i:'category'})
            
    
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID generales
            spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region, i,'species'])##
            sp_numero = spp_numero.groupby([i,region])['species'].count().to_frame(name = 'especies' ).reset_index()
            sp_numero['thematic']=i
            sp_numero=sp_numero.rename(columns={i:'category'})
        
        ##Condicional para registros marinos, continentales y salobres
        if tipo =='MCDM' or tipo =='DCDM':
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría marinos
            sp_numero_marino=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region, i,'species'])##
            sp_numero_marino = sp_numero_marino.groupby([i,region,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
            #sp_numero_marino = variable_conteos_marinos.groupby([i,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
            sp_numero_marino['thematic']=i
            sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})    
    
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría continental
            sp_numero_continental=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region, i,'species'])##
            sp_numero_continental = sp_numero_continental.groupby([i,region,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            #sp_numero_continental = variable_conteos_continentales.groupby([i,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            sp_numero_continental['thematic']=i
            sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})  
    
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría salobre
            sp_numero_salobre=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region, i,'species'])##
            sp_numero_salobre = sp_numero_salobre.groupby([i,region,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            #sp_numero_salobre = variable_conteos_salobres.groupby([i,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            sp_numero_salobre['thematic']=i
            sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})  
    
            ##Agrupar las cifras marinas, continentales y salobres
            sp_tematica=pd.merge(sp_numero,sp_numero_continental, on=['category','thematic',region],how='left').merge(sp_numero_marino, on=['category','thematic',region],how='left').merge(sp_numero_salobre, on=['category','thematic',region],how='left')
            sp_tematica_total=pd.concat([sp_tematica_total,sp_tematica])    
    
        ##Condicional para registros marinos, continentales y salobres
        if tipo =='CCDM':
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría marinos
            sp_numero_marino=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([i,'species'])##
            sp_numero_marino = sp_numero_marino.groupby([i,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
            #sp_numero_marino = variable_conteos_marinos.groupby([i,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
            sp_numero_marino['thematic']=i
            sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})    
    
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría continental
            sp_numero_continental=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([i,'species'])##
            sp_numero_continental = sp_numero_continental.groupby([i,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            #sp_numero_continental = variable_conteos_continentales.groupby([i,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
            sp_numero_continental['thematic']=i
            sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})  
    
            ## Cifras especies: Agrupar por categorías temáticas y contar por gbifID para la categoría salobre
            sp_numero_salobre=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([i,'species'])##
            sp_numero_salobre = sp_numero_salobre.groupby([i,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            #sp_numero_salobre = variable_conteos_salobres.groupby([i,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
            sp_numero_salobre['thematic']=i
            sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})  
    
            ##Agrupar las cifras marinas, continentales y salobres
            sp_tematica=pd.merge(sp_numero,sp_numero_continental, on=['category','thematic'],how='left').merge(sp_numero_marino, on=['category','thematic'],how='left').merge(sp_numero_salobre, on=['category','thematic'],how='left')
            sp_tematica_total=pd.concat([sp_tematica_total,sp_tematica]) 
            
        if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
            sp_tematica_total=pd.concat([sp_tematica_total,sp_numero])
       
    
        ## Cifras registros por entidad geográfica: Agrupar por geografía y categorías temáticas, contar por temática generales
        geo_rb= registros.groupby([region,i])[i].count().to_frame(name = 'registros').reset_index()
        geo_rb['thematic']=i
        geo_rb=geo_rb.rename(columns={i:'category'})
    
    
     ##Condicional para registros marinos, continentales y salobres
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
            ## Cifras registros por entidad geográfica: Agrupar por geografía y categorías temáticas, contar por temática para marinos
            geo_rb_marino= registros_marinos.groupby([region,i,'isMarine'])[i].count().to_frame(name = 'registrosMarinos').reset_index().drop(['isMarine'], axis=1)
            geo_rb_marino['thematic']=i
            geo_rb_marino=geo_rb_marino.rename(columns={i:'category'})
    
            ## Cifras registros por entidad geográfica: Agrupar por geografía y categorías temáticas, contar por temática para continentales
            geo_rb_continental= registros_continentales.groupby([region,i,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales').reset_index().drop(['isTerrestrial'], axis=1)
            geo_rb_continental['thematic']=i
            geo_rb_continental=geo_rb_continental.rename(columns={i:'category'})
    
            ## Cifras registros por entidad geográfica: Agrupar por geografía y categorías temáticas, contar por temática para salobres
            geo_rb_salobre= registros_salobres.groupby([region,i,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres').reset_index().drop(['isBrackish'], axis=1)
            geo_rb_salobre['thematic']=i
            geo_rb_salobre=geo_rb_salobre.rename(columns={i:'category'})
    
            ##Agrupar las cifras marinas, continentales y salobres
            geo_categoria_rb=pd.merge(geo_rb,geo_rb_continental, on=[region,'category','thematic'],how='left').merge(geo_rb_marino, on=[region,'category','thematic'],how='left').merge(geo_rb_salobre, on=[region,'category','thematic'],how='left')
            geo_categoria_rb_total=pd.concat([geo_categoria_rb_total,geo_categoria_rb])
    
        
        else:
            geo_categoria_rb_total=pd.concat([geo_categoria_rb_total,geo_rb])
        
        
        
        ## Cifras especies por entidad geográfica: Agrupar por geografía y categorías temáticas y categorías de cada temática general categorías temáticas, contar por temática para marinos, continentales y salobres
        geo_sp= registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species']).size().reset_index().groupby([region,i]).size().to_frame(name = 'especies').reset_index()
        geo_sp['thematic']=i
        geo_sp= geo_sp.rename(columns={i:'category'})
    
    
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
            
            ## Cifras especies por entidad geográfica: Agrupar por geografia y categorias temáticas y categorías de cada temática para marinos
            geo_sp_marino= registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isMarine']).size().reset_index().groupby([region,i]).size().to_frame(name = 'especiesMarinas').reset_index()
            geo_sp_marino['thematic']=i
            geo_sp_marino= geo_sp_marino.rename(columns={i:'category'})
    
            ## Cifras especies por entidad geográfica: Agrupar por geografia y categorias temáticas y categorías de cada temática para continentales
            geo_sp_continental= registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isTerrestrial']).size().reset_index().groupby([region,i]).size().to_frame(name = 'especiesContinentales').reset_index()
            geo_sp_continental['thematic']=i
            geo_sp_continental= geo_sp_continental.rename(columns={i:'category'})
    
            ## Cifras especies por entidad geográfica: Agrupar por geografia y categorias temáticas y categorías de cada temática para salobres
            geo_sp_salobre= registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isBrackish']).size().reset_index().groupby([region,i]).size().to_frame(name = 'especiesSalobres').reset_index()
            geo_sp_salobre['thematic']=i
            geo_sp_salobre= geo_sp_salobre.rename(columns={i:'category'})
        
            ##Agrupar las cifras marinas, continentales y salobres
            geo_categoria_sp=pd.merge(geo_sp,geo_sp_continental, on=[region,'category','thematic'],how='left').merge(geo_sp_marino, on=[region,'category','thematic'],how='left').merge(geo_sp_salobre, on=[region,'category','thematic'],how='left')
            geo_categoria_sp_total=pd.concat([geo_categoria_sp_total,geo_categoria_sp])
        
        
        else:
            geo_categoria_sp_total=pd.concat([geo_categoria_sp_total,geo_sp])
    
    
        if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM' or tipo =='CSDM':        
            ## Conteo de total registros biológicos para todas las categorías de cada temática general
            geo_rb = registros.groupby([region])[i].count().to_frame(name = 'registros').reset_index()
            geo_rb['thematic']=i
            geo_rb=geo_rb.rename(columns={i:'category'})
    
     ##Condicional para registros marinos, continentales y salobres
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
     
            ## Conteo de total registros biológicos para todas las categorías de cada temática para marinos
            geo_rb_marino = registros_marinos.groupby([region,'isMarine'])[i].count().to_frame(name = 'registrosMarinos').reset_index().drop(['isMarine'], axis=1)
            geo_rb_marino['thematic']=i
            geo_rb_marino=geo_rb_marino.rename(columns={i:'category'})
    
            ## Conteo de total registros biológicos para todas las categorías de cada temática para continentales
            geo_rb_continental = registros_continentales.groupby([region,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales').reset_index().drop(['isTerrestrial'], axis=1)
            geo_rb_continental['thematic']=i
            geo_rb_continental=geo_rb_continental.rename(columns={i:'category'})
    
            ## Conteo de total registros biológicos para todas las categorías de cada temática para salobres
            geo_rb_salobre = registros_salobres.groupby([region,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres').reset_index().drop(['isBrackish'], axis=1)
            geo_rb_salobre['thematic']=i
            geo_rb_salobre=geo_rb_salobre.rename(columns={i:'category'})    
        
            ##Agrupar las cifras marinas, continentales y salobres
            geo_the_rb=pd.merge(geo_rb,geo_rb_continental, on=[region,'thematic'],how='left').merge(geo_rb_marino, on=[region,'thematic'],how='left').merge(geo_rb_salobre, on=[region,'thematic'],how='left')
            geo_tematica_rb_total=pd.concat([geo_tematica_rb_total,geo_the_rb])
    
        if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
            geo_tematica_rb_total=pd.concat([geo_tematica_rb_total,geo_rb])
    
    
        ## Conteo de total especies únicas para todas las categorías temáticas generales
        geo_sp= registros[registros['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species']).size().reset_index().groupby([region])[i].size().to_frame(name = 'especies').reset_index()         
        geo_sp['thematic']=i
        geo_sp=geo_sp.rename(columns={i:'category'})
    
        ##Condicional para registros marinos, continentales y salobres
        if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
    
            ## Conteo de total especies únicas para todas las categorias para marinos
            geo_sp_marino= registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isMarine']).size().reset_index().groupby([region])[i].count().to_frame(name = 'especiesMarinas').reset_index()
            geo_sp_marino['thematic']=i
            geo_sp_marino=geo_sp_marino.rename(columns={i:'category'})
    
            ## Conteo de total especies únicas para todas las categorias para continentales
            geo_sp_continental= registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isTerrestrial']).size().reset_index().groupby([region])[i].count().to_frame(name = 'especiesContinentales').reset_index()
            geo_sp_continental['thematic']=i
            geo_sp_continental=geo_sp_continental.rename(columns={i:'category'})
    
            ## Conteo de total especies únicas para todas las categorias para salobres
            geo_sp_salobre= registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].groupby([region,i,'species','isBrackish']).size().reset_index().groupby([region])[i].count().to_frame(name = 'especiesSalobres').reset_index()
            geo_sp_salobre['thematic']=i
            geo_sp_salobre=geo_sp_salobre.rename(columns={i:'category'})
    
            ##Agrupar las cifras marinas, continentales y salobres
            geo_the_sp=pd.merge(geo_sp,geo_sp_continental, on=[region,'thematic'],how='left').merge(geo_sp_marino, on=[region,'thematic'],how='left').merge(geo_sp_salobre, on=[region,'thematic'],how='left')
            geo_tematica_sp_total=pd.concat([geo_tematica_sp_total,geo_the_sp])
            
        
        
        if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
            geo_tematica_sp_total=pd.concat([geo_tematica_sp_total,geo_sp])
        
        del geo_sp
        ## La segunda parte del loop recorre los grupos taxonómicos
        for j in taxon:    
            
            if tipo =='CCDM' or tipo =='CSDM': 
                ##Número de registros por grupo taxonómico general
                rb_numero = registros.groupby(j)['gbifID'].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'}) 
            
            if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
                ##Número de registros por grupo taxonómico general
                rb_numero = registros.groupby([j,region])['gbifID'].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'}) 
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='CCDM': 
    
                ##Número de registros por grupo taxonómico para marinos
                rb_numero_marino = registros_marinos.groupby([j,'isMarine'])['gbifID'].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'}) 
    
                ##Número de registros por grupo taxonómico para continentales
                rb_numero_continental = registros_continentales.groupby([j,'isTerrestrial'])['gbifID'].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'}) 
    
                ##Número de registros por grupo taxonómico para salobres
                rb_num_salobre = registros_salobres.groupby([j,'isBrackish'])['gbifID'].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre ['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'}) 
    
                ##Agrupar las cifras marinas, continentales y salobres
                rb_tax=pd.merge(rb_numero,rb_numero_continental, on=['taxonRank','grupoTax'],how='left').merge(rb_numero_marino, on=['taxonRank','grupoTax'],how='left').merge(rb_num_salobre, on=['taxonRank','grupoTax'],how='left')
                rb_taxon_total=pd.concat([rb_taxon_total,rb_tax])
                
                    ##Condicional para registros marinos, continentales y salobres
            if tipo =='MCDM' or tipo =='DCDM': 
    
                ##Número de registros por grupo taxonómico para marinos
                rb_numero_marino = registros_marinos.groupby([j,'isMarine',region])['gbifID'].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'}) 
    
                ##Número de registros por grupo taxonómico para continentales
                rb_numero_continental = registros_continentales.groupby([j,'isTerrestrial',region])['gbifID'].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'}) 
    
                ##Número de registros por grupo taxonómico para salobres
                rb_num_salobre = registros_salobres.groupby([j,'isBrackish',region])['gbifID'].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre ['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'}) 
    
                ##Agrupar las cifras marinas, continentales y salobres
                rb_tax=pd.merge(rb_numero,rb_numero_continental, on=['taxonRank','grupoTax',region],how='left').merge(rb_numero_marino, on=['taxonRank','grupoTax',region],how='left').merge(rb_num_salobre, on=['taxonRank','grupoTax',region],how='left')
                rb_taxon_total=pd.concat([rb_taxon_total,rb_tax])
                
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                rb_taxon_total=pd.concat([rb_taxon_total,rb_numero])
    
            
            if tipo =='CCDM' or tipo =='CSDM': 
                ## Número de registros por grupo taxonómico en la categoría general
                rb_numero = registros.groupby(j)[i].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['thematic']=i
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'})
                rb_numero=rb_numero.rename(columns={i:'category'})
            
            if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM': 
                ## Número de registros por grupo taxonómico en la categoría general
                rb_numero = registros.groupby([j,region])[i].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['thematic']=i
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'})
                rb_numero=rb_numero.rename(columns={i:'category'})
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='CCDM':   
                ## Número de registros por grupo taxonómico en la categoría para marinos
                rb_numero_marino = registros_marinos.groupby([j,'isMarine'])[i].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['thematic']=i
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'})
                rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico en la categoría para continentales
                rb_numero_continental = registros_continentales.groupby([j,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['thematic']=i
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'})
                rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico en la categoría para salobres
                rb_num_salobre = registros_salobres.groupby([j,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre['thematic']=i
                rb_num_salobre['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'})
                rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})        
            
                ##Agrupar las cifras marinas, continentales y salobres
                rb_tax_the=pd.merge(rb_numero,rb_numero_continental, on=['taxonRank','grupoTax','thematic'],how='left').merge(rb_numero_marino, on=['taxonRank','grupoTax','thematic'],how='left').merge(rb_num_salobre, on=['taxonRank','grupoTax','thematic'],how='left')
                rb_taxon_tematica_total=pd.concat([rb_taxon_tematica_total,rb_tax_the])
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='MCDM' or tipo =='DCDM':   
                ## Número de registros por grupo taxonómico en la categoría para marinos
                rb_numero_marino = registros_marinos.groupby([region,j,'isMarine'])[i].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['thematic']=i
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'})
                rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico en la categoría para continentales
                rb_numero_continental = registros_continentales.groupby([region,j,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['thematic']=i
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'})
                rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico en la categoría para salobres
                rb_num_salobre = registros_salobres.groupby([region,j,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre['thematic']=i
                rb_num_salobre['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'})
                rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})        
            
                ##Agrupar las cifras marinas, continentales y salobres
                rb_tax_the=pd.merge(rb_numero,rb_numero_continental, on=[region,'taxonRank','grupoTax','thematic'],how='left').merge(rb_numero_marino, on=[region,'taxonRank','grupoTax','thematic'],how='left').merge(rb_num_salobre, on=[region,'taxonRank','grupoTax','thematic'],how='left')
                rb_taxon_tematica_total=pd.concat([rb_taxon_tematica_total,rb_tax_the])
    
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                rb_taxon_tematica_total=pd.concat([rb_taxon_tematica_total,rb_numero])
    
    
    
            if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
                ## Número de registros por grupo taxonómico y categoría temática en general
                rb_numero = registros.groupby([j,i,region])[i].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['thematic']=i
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'})
                rb_numero=rb_numero.rename(columns={i:'category'})
                
            if tipo =='CCDM' or tipo =='CSDM':
                ## Número de registros por grupo taxonómico y categoría temática en general
                rb_numero = registros.groupby([j,i])[i].count().to_frame(name = 'registros' ).reset_index()
                rb_numero['thematic']=i
                rb_numero['taxonRank']=j
                rb_numero=rb_numero.rename(columns={j:'grupoTax'})
                rb_numero=rb_numero.rename(columns={i:'category'})
    
            if tipo =='CCDM':
                ## Número de registros por grupo taxonómico y categoría temática para marinos
                rb_numero_marino = registros_marinos.groupby([j,i,'isMarine'])[i].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['thematic']=i
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'})
                rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico y categoría temática para continentales
                rb_numero_continental = registros_continentales.groupby([j,i,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['thematic']=i
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'})
                rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})    
    
                ## Número de registros por grupo taxonómico y categoría temática para salobres
                rb_num_salobre = registros_salobres.groupby([j,i,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre['thematic']=i
                rb_num_salobre['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'})
                rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})    
    
                ##Agrupar las cifras marinas, continentales y salobres
                rb_taxon_categoria=pd.merge(rb_numero,rb_numero_continental, on=['taxonRank','grupoTax','category','thematic'],how='left').merge(rb_numero_marino, on=['taxonRank','grupoTax','category','thematic'],how='left').merge(rb_num_salobre, on=['taxonRank','grupoTax','category','thematic'],how='left') 
                rb_taxon_categoria_total=pd.concat([rb_taxon_categoria_total,rb_taxon_categoria])
    
            if tipo =='MCDM' or tipo =='DCDM':
                ## Número de registros por grupo taxonómico y categoría temática para marinos
                rb_numero_marino = registros_marinos.groupby([region,j,i,'isMarine'])[i].count().to_frame(name = 'registrosMarinos' ).reset_index().drop(['isMarine'], axis=1)
                rb_numero_marino['thematic']=i
                rb_numero_marino['taxonRank']=j
                rb_numero_marino=rb_numero_marino.rename(columns={j:'grupoTax'})
                rb_numero_marino=rb_numero_marino.rename(columns={i:'category'})
    
                ## Número de registros por grupo taxonómico y categoría temática para continentales
                rb_numero_continental = registros_continentales.groupby([region,j,i,'isTerrestrial'])[i].count().to_frame(name = 'registrosContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                rb_numero_continental['thematic']=i
                rb_numero_continental['taxonRank']=j
                rb_numero_continental=rb_numero_continental.rename(columns={j:'grupoTax'})
                rb_numero_continental=rb_numero_continental.rename(columns={i:'category'})    
    
                ## Número de registros por grupo taxonómico y categoría temática para salobres
                rb_num_salobre = registros_salobres.groupby([region,j,i,'isBrackish'])[i].count().to_frame(name = 'registrosSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                rb_num_salobre['thematic']=i
                rb_num_salobre['taxonRank']=j
                rb_num_salobre=rb_num_salobre.rename(columns={j:'grupoTax'})
                rb_num_salobre=rb_num_salobre.rename(columns={i:'category'})    
    
                ##Agrupar las cifras marinas, continentales y salobres
                rb_taxon_categoria=pd.merge(rb_numero,rb_numero_continental, on=[region,'taxonRank','grupoTax','category','thematic'],how='left').merge(rb_numero_marino, on=[region,'taxonRank','grupoTax','category','thematic'],how='left').merge(rb_num_salobre, on=[region,'taxonRank','grupoTax','category','thematic'],how='left') 
                rb_taxon_categoria_total=pd.concat([rb_taxon_categoria_total,rb_taxon_categoria])
    
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                rb_taxon_categoria_total=pd.concat([rb_taxon_categoria_total,rb_numero])
    

            
            ## Especies 
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
                ## Número de especies por grupo taxonómico general
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region, j,'species'])##
                sp_numero = spp_numero.groupby([j,region])['species'].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
            
            if tipo =='CSDM' or tipo =='CCDM':
                ## Número de especies por grupo taxonómico general
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero = spp_numero.groupby([j])['species'].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
                
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='CCDM':
                ## Número de especies por grupo taxonómico para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_marino = spp_numero.groupby([j,'isMarine'])['species'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([j,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})        
    
                ## Número de especies por grupo taxonómico para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_continental = spp_numero.groupby([j,'isTerrestrial'])['species'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([j,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})   
    
                ## Número de especies por grupo taxonómico para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_salobre = spp_numero.groupby([j,'isBrackish'])['species'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([j,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})   
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon=pd.merge(sp_numero,sp_numero_continental, on=['taxonRank','grupoTax'],how='left').merge(sp_numero_marino, on=['taxonRank','grupoTax'],how='left').merge(sp_numero_salobre, on=['taxonRank','grupoTax'],how='left')  
                sp_taxon_total=pd.concat([sp_taxon_total,sp_taxon])   
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='MCDM' or tipo =='DCDM':
                ## Número de especies por grupo taxonómico para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_marino = spp_numero.groupby([region,j,'isMarine'])['species'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([region,j,'isMarine'])['gbifID'].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})        
    
                ## Número de especies por grupo taxonómico para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_continental = spp_numero.groupby([region,j,'isTerrestrial'])['species'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([region,j,'isTerrestrial'])['gbifID'].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})   
    
                ## Número de especies por grupo taxonómico para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_salobre = spp_numero.groupby([region,j,'isBrackish'])['species'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([region,j,'isBrackish'])['gbifID'].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})   
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon=pd.merge(sp_numero,sp_numero_continental, on=[region,'taxonRank','grupoTax'],how='left').merge(sp_numero_marino, on=[region,'taxonRank','grupoTax'],how='left').merge(sp_numero_salobre, on=[region,'taxonRank','grupoTax'],how='left')  
                sp_taxon_total=pd.concat([sp_taxon_total,sp_taxon])   
            
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                sp_taxon_total=pd.concat([sp_taxon_total,sp_numero])
    
            
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
                ## Número de especies por grupo taxonómico, categoría y temática, general
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species',i])
                sp_numero = spp_numero.groupby([j,region])[i].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['thematic']=i
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
                sp_numero=sp_numero.rename(columns={i:'category'})
    
            if tipo =='CSDM' or tipo =='CCDM':
                ## Número de especies por grupo taxonómico, categoría y temática, general
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species',i])
                sp_numero = spp_numero.groupby([j])[i].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['thematic']=i
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
                sp_numero=sp_numero.rename(columns={i:'category'})


    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='CCDM':
                ## Número de especies por grupo taxonómico, categoría y temática para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_marino = spp_numero.groupby([j,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([j,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['thematic']=i
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})
                sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría y temática para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_continental = spp_numero.groupby([j,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([j,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['thematic']=i
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})
                sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría y temática para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_salobre = spp_numero.groupby([j,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([j,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['thematic']=i
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})
                sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon_tematica=pd.merge(sp_numero,sp_numero_continental, on=['taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_marino, on=['taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_salobre, on=['taxonRank','thematic','grupoTax'],how='left')                
                sp_taxon_tematica_total=pd.concat([sp_taxon_tematica_total,sp_taxon_tematica]) 
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='MCDM' or tipo =='DCDM':
                ## Número de especies por grupo taxonómico, categoría y temática para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_marino = spp_numero.groupby([region,j,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([region,j,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['thematic']=i
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})
                sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría y temática para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_continental = spp_numero.groupby([region,j,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([region,j,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['thematic']=i
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})
                sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría y temática para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_salobre = spp_numero.groupby([region,j,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([region,j,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['thematic']=i
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})
                sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon_tematica=pd.merge(sp_numero,sp_numero_continental, on=[region,'taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_marino, on=[region,'taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_salobre, on=[region,'taxonRank','thematic','grupoTax'],how='left')                
                sp_taxon_tematica_total=pd.concat([sp_taxon_tematica_total,sp_taxon_tematica]) 
    
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                sp_taxon_tematica_total=pd.concat([sp_taxon_tematica_total,sp_numero]) 
    
            ## Número de especies por grupo taxonómico, categoría temática y temática, general
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species',i])##
                sp_numero = spp_numero.groupby([j,i,region])[i].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['thematic']=i
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
                sp_numero=sp_numero.rename(columns={i:'category'})
        
            if tipo =='CSDM' or tipo =='CCDM':
                ## Número de especies por grupo taxonómico, categoría temática y temática, general
                spp_numero=registros[registros['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species',i])##
                sp_numero = spp_numero.groupby([j,i])[i].count().to_frame(name = 'especies' ).reset_index()
                sp_numero['thematic']=i
                sp_numero['taxonRank']=j
                sp_numero=sp_numero.rename(columns={j:'grupoTax'})
                sp_numero=sp_numero.rename(columns={i:'category'})
                #print(sp_numero)
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='CCDM':
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_marino = spp_numero.groupby([j,i,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([j,i,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['thematic']=i
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})
                sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_continental = spp_numero.groupby([j,i,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([j,i,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['thematic']=i
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})
                sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([j,'species'])##
                sp_numero_salobre = spp_numero.groupby([j,i,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([j,i,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['thematic']=i
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})
                sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon_categoria=pd.merge(sp_numero,sp_numero_continental, on=['category','taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_marino, on=['category','taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_salobre, on=['category','taxonRank','thematic','grupoTax'],how='left')  
                sp_taxon_categoria_total=pd.concat([sp_taxon_categoria_total,sp_taxon_categoria])
    
            ##Condicional para registros marinos, continentales y salobres
            if tipo =='MCDM' or tipo =='DCDM':
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para marinos
                spp_numero=registros_marinos[registros_marinos['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_marino = spp_numero.groupby([region,j,i,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                #sp_numero_marino = variable_conteos_marinos.groupby([region,j,i,'isMarine'])[i].count().to_frame(name = 'especiesMarinas' ).reset_index().drop(['isMarine'], axis=1)
                sp_numero_marino['thematic']=i
                sp_numero_marino['taxonRank']=j
                sp_numero_marino=sp_numero_marino.rename(columns={j:'grupoTax'})
                sp_numero_marino=sp_numero_marino.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para continentales
                spp_numero=registros_continentales[registros_continentales['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_continental = spp_numero.groupby([region,j,i,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                #sp_numero_continental = variable_conteos_continentales.groupby([region,j,i,'isTerrestrial'])[i].count().to_frame(name = 'especiesContinentales' ).reset_index().drop(['isTerrestrial'], axis=1)
                sp_numero_continental['thematic']=i
                sp_numero_continental['taxonRank']=j
                sp_numero_continental=sp_numero_continental.rename(columns={j:'grupoTax'})
                sp_numero_continental=sp_numero_continental.rename(columns={i:'category'})
    
                ## Número de especies por grupo taxonómico, categoría temática y temática, general para salobres
                spp_numero=registros_salobres[registros_salobres['flagTAXO']!='Ausente en lista taxonómica'].drop_duplicates([region,j,'species'])##
                sp_numero_salobre = spp_numero.groupby([region,j,i,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                #sp_numero_salobre = variable_conteos_salobres.groupby([region,j,i,'isBrackish'])[i].count().to_frame(name = 'especiesSalobres' ).reset_index().drop(['isBrackish'], axis=1)
                sp_numero_salobre['thematic']=i
                sp_numero_salobre['taxonRank']=j
                sp_numero_salobre=sp_numero_salobre.rename(columns={j:'grupoTax'})
                sp_numero_salobre=sp_numero_salobre.rename(columns={i:'category'})
    
                ##Agrupar las cifras marinas, continentales y salobres
                sp_taxon_categoria=pd.merge(sp_numero,sp_numero_continental, on=[region,'category','taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_marino, on=[region,'category','taxonRank','thematic','grupoTax'],how='left').merge(sp_numero_salobre, on=[region,'category','taxonRank','thematic','grupoTax'],how='left')  
                sp_taxon_categoria_total=pd.concat([sp_taxon_categoria_total,sp_taxon_categoria])
    
            if tipo =='MSDM' or tipo =='DSDM' or tipo =='CSDM':
                sp_taxon_categoria_total=pd.concat([sp_taxon_categoria_total,sp_numero])



    ##Fin del loop para realizar conteos
    
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
        ## Agrupar la información: Número de registros y especies por grupo taxonómico en la temática
        taxon_tematica=pd.merge(rb_taxon_tematica_total,sp_taxon_tematica_total,on=['taxonRank','thematic','grupoTax',region],how='left')
    else:
        ## Agrupar la información: Número de registros y especies por grupo taxonómico en la temática
        taxon_tematica=pd.merge(rb_taxon_tematica_total,sp_taxon_tematica_total,on=['taxonRank','thematic','grupoTax'],how='left')
    
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='MSDM' or tipo =='DSDM':
        ## Agrupar la información: Número de registros y especies por grupo taxonómico en la temática y categoría
        taxon_categoria=pd.merge(rb_taxon_categoria_total,sp_taxon_categoria_total,on=['taxonRank','thematic','grupoTax','category',region],how='left')
    else:
     ## Agrupar la información: Número de registros y especies por grupo taxonómico en la temática y categoría
        taxon_categoria=pd.merge(rb_taxon_categoria_total,sp_taxon_categoria_total,on=['taxonRank','thematic','grupoTax','category'],how='left')    
    ## Guardar archivos de resultado sobre los cuales se puede recalcular cifras según los grupos biológicos
   
    rb_taxon_total.to_csv(nombre+'_registrosBiologicosTaxonTotal.tsv',sep='\t', index=False )
    sp_taxon_total.to_csv(nombre+'_especiesTaxonTotal.tsv',sep='\t', index=False )
    rb_taxon_tematica_total.to_csv(nombre+'_registrosBiologicosTaxonTematicas.tsv',sep='\t', index=False )
    sp_taxon_tematica_total.to_csv(nombre+'_especiesTaxonTematicas.tsv',sep='\t', index=False )
    rb_taxon_categoria_total.to_csv(nombre+'_registrosBiologicosTaxonCategoriasTotal.tsv',sep='\t', index=False )
    sp_taxon_categoria_total.to_csv(nombre+'_especiesTaxonCategoriasTotal.tsv',sep='\t', index=False )             
 
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        del rb_tematica
        del geo_sp_marino
        del geo_sp_continental
        del geo_sp_salobre
        del sp_numero_continental
        del geo_rb_marino
        del geo_rb_salobre
        del geo_rb_continental
        del rb_numero_marino
        del rb_numero_continental
        del rb_num_salobre  
        del sp_numero_marino
        del sp_numero_salobre
        del registros_marinos
        del registros_continentales
        del registros_salobres
        del sp_tematica
        del geo_categoria_sp 
        del rb_tax_the
        del sp_taxon_tematica
        del sp_taxon_categoria
        del rb_taxon_categoria
        
    del rb_tematica_total   
    
    del geo_rb       
    del rb_numero     
    del spp_numero
    #del sp_numero   
    del variable_conteos
    del rb_taxon_tematica_total
    del rb_taxon_categoria_total
    del sp_tematica_total
    #del sp_taxon_tematica_total
    del sp_taxon_categoria_total
    
    
    ##-----------------------------13. Organización en un único dataframe de las cifras geográficas------------------------------------##
    '''
    En esta sección se crea el conjunto de datos final para la obtención de las cifras temáticas para cada una de las entidades geográficas
    Dentro de los nombres se encuentran las siguientes convenciones:
    rb= Registros biologicos
    sp= especies
    geo= geografía
    '''
    
    
    #geo_tematica_rb_total.to_csv('geo_tematica_rb_total.tsv',sep='\t', index=False )
    ## Reorganizar los conjuntos de datos
    
    
    #geo_tematica_rb_total=geo_tematica_rb_total.pivot_table(region,'thematic',aggfunc="mean")
    geo_tematica_rb_total = pd.pivot_table(geo_tematica_rb_total, values='registros', index=region,columns=["thematic"])
    geo_tematica_rb_total=geo_tematica_rb_total.add_prefix('registros ')
    #geo_tematica_sp_total=geo_tematica_sp_total.pivot_table(region,'thematic')
    geo_tematica_sp_total = pd.pivot_table(geo_tematica_sp_total, values='especies', index=region,columns=["thematic"])
    geo_tematica_sp_total=geo_tematica_sp_total.add_prefix('especies ')

 ##Eliminar columna 'thematic' y reordenar
    
    geo_categoria_rb_total=geo_categoria_rb_total.drop(['thematic'],axis=1)
    #geo_categoria_rb_total=geo_categoria_rb_total.pivot_table(region,'category')
    geo_categoria_rb_total = pd.pivot_table(geo_categoria_rb_total, values='registros', index=region,columns=["category"])
    geo_categoria_rb_total=geo_categoria_rb_total.add_prefix('registros ')
    
    geo_categoria_sp_total=geo_categoria_sp_total.drop(['thematic'],axis=1)
    #geo_categoria_sp_total=geo_categoria_sp_total.pivot_table(region,'category')
    geo_categoria_sp_total = pd.pivot_table(geo_categoria_sp_total, values='especies', index=region,columns=["category"])
    geo_categoria_sp_total=geo_categoria_sp_total.add_prefix('especies ')
    
    ##Conjunto de datos final para cifras geográficas
    
    geografia_total=pd.merge(geo_rb_total,geo_sp_total,on=region,how='left').merge(geo_tematica_rb_total,on=region,how='left').merge(geo_tematica_sp_total,on=region,how='left').merge(geo_categoria_rb_total,on=region,how='left').merge(geo_categoria_sp_total,on=region,how='left')
    del geo_rb_total
    del geo_sp_total
    del geo_tematica_rb_total
    del geo_tematica_sp_total
    del geo_categoria_rb_total
    del geo_categoria_sp_total
    
    print(geografia_total.values)
    
    if ('especies Exótica') not in geografia_total.columns:
        geografia_total[('especies Exótica')]=0
    if ('especies Invasora') not in geografia_total.columns:
        geografia_total[('especies Invasora')]=0
    if ('especies Exótica con potencial de invasión') not in geografia_total.columns:
        geografia_total[('especies Exótica con potencial de invasión')]=0
    if ('especies Trasplantada') not in geografia_total.columns:
        geografia_total[('especies Trasplantada')]=0
    if ('especies EN_IUCN') not in geografia_total.columns:
        geografia_total[('especies EN_IUCN')]=0
    if ('especies CR_IUCN') not in geografia_total.columns:
        geografia_total[('especies CR_IUCN')]=0
    if ('especies VU_IUCN') not in geografia_total.columns:
        geografia_total[('especies VU_IUCN')]=0
    #if ('especies', 'EN_IUCN') not in geografia_total.columns:
    #    geografia_total[('especies', 'EN_IUCN')]=0
    #if ('especies', 'CR_IUCN') not in geografia_total.columns:
    #    geografia_total[('especies', 'CR_IUCN')]=0
    #if ('especies', 'VU_IUCN') not in geografia_total.columns:
    #    geografia_total[('especies', 'VU_IUCN')]=0   
    if ('especies Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
        geografia_total[('especies Exótica con potencial de invasión Alto Riesgo')]=0
    if ('especies Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
        geografia_total[('especies Exótica con potencial de invasión Bajo Riesgo')]=0
    if ('especies Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
        geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado')]=0
    if ('especies Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
        geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0
    
    if ('registros Exótica') not in geografia_total.columns:
        geografia_total[('registros Exótica')]=0
    if ('registros Invasora') not in geografia_total.columns:
        geografia_total[('registros Invasora')]=0
    if ('registros Exótica con potencial de invasión') not in geografia_total.columns:
        geografia_total[('registros Exótica con potencial de invasión')]=0
    if ('registros Trasplantada') not in geografia_total.columns:
        geografia_total[('registros Trasplantada')]=0
    if ('registros EN_IUCN') not in geografia_total.columns:
        geografia_total[('registros EN_IUCN')]=0
    if ('registros CR_IUCN') not in geografia_total.columns:
        geografia_total[('registros CR_IUCN')]=0
    if ('registros VU_IUCN') not in geografia_total.columns:
        geografia_total[('registros VU_IUCN')]=0
    #if ('registros', 'EN_IUCN') not in geografia_total.columns:
    #    geografia_total[('registros', 'EN_IUCN')]=0
    #if ('registros', 'CR_IUCN') not in geografia_total.columns:
    #    geografia_total[('registros', 'CR_IUCN')]=0
    #if ('registros', 'VU_IUCN') not in geografia_total.columns:
    #    geografia_total[('registros', 'VU_IUCN')]=0 
    if ('registros Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
        geografia_total[('registros Exótica con potencial de invasión Alto Riesgo')]=0
    if ('registros Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
        geografia_total[('registros Exótica con potencial de invasión Bajo Riesgo')]=0
    if ('registros Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
        geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado')]=0
    if ('registros Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
        geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

        
    geografia_total['especies_exoticas_total']=geografia_total[('especies Exótica')].fillna(value=0)+geografia_total[('especies Invasora')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)+geografia_total[('especies Trasplantada')].fillna(value=0)
    geografia_total['registros_exoticas_total']=geografia_total[('registros Exótica')].fillna(value=0)+geografia_total[('registros Invasora')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)+geografia_total[('registros Trasplantada')].fillna(value=0)
    geografia_total['especies_exoticas_riesgo_invasion_total']=geografia_total[('especies Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
    geografia_total['registros_exoticas_riesgo_invasion_total']=geografia_total[('registros Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
       
    geografia_total['especies_amenazadas_global_total']=geografia_total[('especies EN_IUCN')].fillna(value=0)+geografia_total[('especies CR_IUCN')].fillna(value=0)+geografia_total[('especies VU_IUCN')].fillna(value=0)
    geografia_total['registros_amenazadas_global_total']=geografia_total[('registros EN_IUCN')].fillna(value=0)+geografia_total[('registros CR_IUCN')].fillna(value=0)+geografia_total[('registros VU_IUCN')].fillna(value=0)
    geografia_total[('especies_exoticas_total')]=geografia_total[('especies_exoticas_total')].replace(0,'',regex=True)
    geografia_total[('registros_exoticas_total')]=geografia_total[('registros_exoticas_total')].replace(0,'',regex=True)
    geografia_total[('especies_exoticas_riesgo_invasion_total')]=geografia_total[('especies_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    geografia_total[('registros_exoticas_riesgo_invasion_total')]=geografia_total[('registros_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    geografia_total[('especies_amenazadas_global_total')]=geografia_total[('especies_amenazadas_global_total')].replace(0,'',regex=True)
    geografia_total[('registros_amenazadas_global_total')]=geografia_total[('registros_amenazadas_global_total')].replace(0,'',regex=True)
        
    
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        
        if ('especies Continentales Exóticas') not in geografia_total.columns:
            geografia_total[('especies Continentales Exóticas')]=0
        if ('especies Continentales Invasoras') not in geografia_total.columns:
            geografia_total[('especies Continentales Invasoras')]=0
        if ('especies Continentales Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('especies Continentales Exótica con potencial de invasión')]=0
        if ('especies Continentales Trasplantada') not in geografia_total.columns:
            geografia_total[('especies Continentales Trasplantada')]=0
            
        if ('especies Continentales Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('especies Continentales Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Continentales Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('especies Continentales Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Continentales Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0
        
        
        if ('especies Continentales EN_IUCN') not in geografia_total.columns:
            geografia_total[('especies Continentales EN_IUCN')]=0
        if ('especies Continentales CR_IUCN') not in geografia_total.columns:
            geografia_total[('especies Continentales CR_IUCN')]=0
        if ('especies Continentales VU_IUCN') not in geografia_total.columns:
            geografia_total[('especies Continentales VU_IUCN')]=0
       
        if ('registros Continentales Exóticas') not in geografia_total.columns:
            geografia_total[('registros Continentales Exóticas')]=0
        if ('registros Continentales Invasoras') not in geografia_total.columns:
            geografia_total[('registros Continentales Invasoras')]=0
        if ('registros Continentales Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('registros Continentales Exótica con potencial de invasión')]=0
        if ('registros Continentales Trasplantada') not in geografia_total.columns:
            geografia_total[('registros Continentales Trasplantada')]=0    
            
        if ('registros Continentales Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('registros Continentales Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Continentales Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('registros Continentales Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Continentales Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('registros Continentales EN_IUCN') not in geografia_total.columns:
            geografia_total[('registros Continentales EN_IUCN')]=0
        if ('registros Continentales CR_IUCN') not in geografia_total.columns:
            geografia_total[('registros Continentales CR_IUCN')]=0
        if ('registros Continentales VU_IUCN') not in geografia_total.columns:
            geografia_total[('registros Continentales VU_IUCN')]=0
            
        if ('especies Marinas Exóticas') not in geografia_total.columns:
            geografia_total[('especies Marinas Exóticas')]=0
        if ('especies Marinas Invasoras') not in geografia_total.columns:
            geografia_total[('especies Marinas Invasoras')]=0
        if ('especies Marinas Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('especies Marinas Exótica con potencial de invasión')]=0
        if ('especies Marinas Trasplantada') not in geografia_total.columns:
            geografia_total[('especies Marinas Trasplantada')]=0  
        
        if ('especies Marinas Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('especies Marinas Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Marinas Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('especies Marinas Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Marinas Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0
        
            
        if ('especies Marinas EN_IUCN') not in geografia_total.columns:
            geografia_total[('especies Marinas EN_IUCN')]=0
        if ('especies Marinas CR_IUCN') not in geografia_total.columns:
            geografia_total[('especies Marinas CR_IUCN')]=0
        if ('especies Marinas VU_IUCN') not in geografia_total.columns:
            geografia_total[('especies Marinas VU_IUCN')]=0
        
        if ('registros Marinos Exóticas') not in geografia_total.columns:
            geografia_total[('registros Marinos Exóticas')]=0
        if ('registros Marinos Invasoras') not in geografia_total.columns:
            geografia_total[('registros Marinos Invasoras')]=0
        if ('registros Marinos Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('registros Marinos Exótica con potencial de invasión')]=0
        if ('registros Marinos Trasplantada') not in geografia_total.columns:
            geografia_total[('registros Marinos Trasplantada')]=0   
            
        if ('registros Marinos Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('registros Marinos Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Marinos Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('registros Marinos Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Marinos Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0
        
            
        if ('registros Marinos EN_IUCN') not in geografia_total.columns:
            geografia_total[('registros Marinos EN_IUCN')]=0
        if ('registros Marinos CR_IUCN') not in geografia_total.columns:
            geografia_total[('registros Marinos CR_IUCN')]=0
        if ('registros Marinos VU_IUCN') not in geografia_total.columns:
            geografia_total[('registros Marinos VU_IUCN')]=0

        if ('especies Salobres Exóticas') not in geografia_total.columns:
            geografia_total[('especies Salobres Exóticas')]=0
        if ('especies Salobres Invasoras') not in geografia_total.columns:
            geografia_total[('especies Salobres Invasoras')]=0
        if ('especies Salobres Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('especies Salobres Exótica con potencial de invasión')]=0
        if ('especies Salobres Trasplantada') not in geografia_total.columns:
            geografia_total[('especies Salobres Trasplantada')]=0     

        if ('especies Salobres Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('especies Salobres Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Salobres Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('especies Salobres Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Salobres Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('especies Salobres EN_IUCN') not in geografia_total.columns:
            geografia_total[('especies Salobres EN_IUCN')]=0
        if ('especies Salobres CR_IUCN') not in geografia_total.columns:
            geografia_total[('especies Salobres CR_IUCN')]=0
        if ('especies Salobres VU_IUCN') not in geografia_total.columns:
            geografia_total[('especies Salobres VU_IUCN')]=0
        
        if ('registros Salobres Exóticas') not in geografia_total.columns:
            geografia_total[('registros Salobres Exóticas')]=0
        if ('registros Salobres Invasoras') not in geografia_total.columns:
            geografia_total[('registros Salobres Invasoras')]=0
        if ('registros Salobres Exótica con potencial de invasión') not in geografia_total.columns:
            geografia_total[('registros Salobres Exótica con potencial de invasión')]=0
        if ('registros Salobres Trasplantada') not in geografia_total.columns:
            geografia_total[('registros Salobres Trasplantada')]=0          

        if ('registros Salobres Exótica con potencial de invasión Alto Riesgo') not in geografia_total.columns:
            geografia_total[('registros Salobres Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Salobres Exótica con potencial de invasión Bajo Riesgo') not in geografia_total.columns:
            geografia_total[('registros Salobres Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Salobres Exótica con potencial de invasión Riesgo Moderado') not in geografia_total.columns:
            geografia_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto') not in geografia_total.columns:
            geografia_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('registros Salobres EN_IUCN') not in geografia_total.columns:
            geografia_total[('registros Salobres EN_IUCN')]=0
        if ('registros Salobres CR_IUCN') not in geografia_total.columns:
            geografia_total[('registros Salobres CR_IUCN')]=0
        if ('registros Salobres VU_IUCN') not in geografia_total.columns:
            geografia_total[('registros Salobres VU_IUCN')]=0
            
        geografia_total['especies_continentales_exoticas_total']=geografia_total[('especies Continentales Exóticas')].fillna(value=0)+geografia_total[('especies Continentales Invasoras')].fillna(value=0)+geografia_total[('especies Continentales Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('especies Continentales Trasplantada')].fillna(value=0)
        geografia_total['registros_continentales_exoticas_total']=geografia_total[('registros Continentales Exóticas')].fillna(value=0)+geografia_total[('registros Continentales Invasoras')].fillna(value=0)+geografia_total[('registros Continentales Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('registros Continentales Trasplantada')].fillna(value=0)
        geografia_total['especies_marinas_exoticas_total']=geografia_total[('especies Marinas Exóticas')].fillna(value=0)+geografia_total[('especies Marinas Invasoras')].fillna(value=0)+geografia_total[('especies Marinas Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('especies Marinas Trasplantada')].fillna(value=0)
        geografia_total['registros_marinas_exoticas_total']=geografia_total[('registros Marinos Exóticas')].fillna(value=0)+geografia_total[('registros Marinos Invasoras')].fillna(value=0)+geografia_total[('registros Marinos Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('registros Marinos Trasplantada')].fillna(value=0)
        geografia_total['especies_salobres_exoticas_total']=geografia_total[('especies Salobres Exóticas')].fillna(value=0)+geografia_total[('especies Salobres Invasoras')].fillna(value=0)+geografia_total[('especies Salobres Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('especies Salobres Trasplantada')].fillna(value=0)
        geografia_total['registros_salobres_exoticas_total']=geografia_total[('registros Salobres Exóticas')].fillna(value=0)+geografia_total[('registros Salobres Invasoras')].fillna(value=0)+geografia_total[('registros Salobres Exótica con potencial de invasión')].fillna(value=0)+geografia_total[('registros Salobres Trasplantada')].fillna(value=0)
        
        geografia_total['especies_continentales_exoticas_riesgo_invasion_total']=geografia_total[('especies Continentales Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('especies Continentales Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        geografia_total['registros_continentales_exoticas_riesgo_invasion_total']=geografia_total[('registros Continentales Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('registros Continentales Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        geografia_total['especies_marinas_exoticas_riesgo_invasion_total']=geografia_total[('especies Marinas Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('especies Marinas Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        geografia_total['registros_marinas_exoticas_riesgo_invasion_total']=geografia_total[('registros Marinos Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('registros Marinos Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        geografia_total['especies_salobres_exoticas_riesgo_invasion_total']=geografia_total[('especies Salobres Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('especies Salobres Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        geografia_total['registros_salobres_exoticas_riesgo_invasion_total']=geografia_total[('registros Salobres Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+geografia_total[('registros Salobres Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+geografia_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+geografia_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
                              
        geografia_total[('especies_continentales_exoticas_total')]=geografia_total[('especies_continentales_exoticas_total')].replace(0,'',regex=True)
        geografia_total[('registros_continentales_exoticas_total')]=geografia_total[('registros_continentales_exoticas_total')].replace(0,'',regex=True)
        geografia_total[('especies_marinas_exoticas_total')]=geografia_total[('especies_marinas_exoticas_total')].replace(0,'',regex=True)
        geografia_total[('registros_marinas_exoticas_total')]=geografia_total[('registros_marinas_exoticas_total')].replace(0,'',regex=True)
        geografia_total[('especies_salobres_exoticas_total')]=geografia_total[('especies_salobres_exoticas_total')].replace(0,'',regex=True)
        geografia_total[('registros_salobres_exoticas_total')]=geografia_total[('registros_salobres_exoticas_total')].replace(0,'',regex=True)
        
        geografia_total[('especies_continentales_exoticas_riesgo_invasion_total')]=geografia_total[('especies_continentales_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        geografia_total[('registros_continentales_exoticas_riesgo_invasion_total')]=geografia_total[('registros_continentales_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        geografia_total[('especies_marinas_exoticas_riesgo_invasion_total')]=geografia_total[('especies_marinas_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        geografia_total[('registros_marinas_exoticas_riesgo_invasion_total')]=geografia_total[('registros_marinas_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        geografia_total[('especies_salobres_exoticas_riesgo_invasion_total')]=geografia_total[('especies_salobres_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        geografia_total[('registros_salobres_exoticas_riesgo_invasion_total')]=geografia_total[('registros_salobres_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    
        geografia_total[('especies_marinas_amenazadas_global_total')]=geografia_total[('especies Marinas EN_IUCN')].fillna(value=0)+geografia_total[('especies Marinas CR_IUCN')].fillna(value=0)+geografia_total[('especies Marinas VU_IUCN')].fillna(value=0)
        geografia_total[('registros_marinas_amenazadas_global_total')]=geografia_total[('registros Marinos EN_IUCN')].fillna(value=0)+geografia_total[('registros Marinos CR_IUCN')].fillna(value=0)+geografia_total[('registros Marinos VU_IUCN')].fillna(value=0)
        geografia_total[('especies_continentales_amenazadas_global_total')]=geografia_total[('especies Continentales EN_IUCN')].fillna(value=0)+geografia_total[('especies Continentales CR_IUCN')].fillna(value=0)+geografia_total[('especies Continentales VU_IUCN')].fillna(value=0)
        geografia_total[('registros_continentales_amenazadas_global_total')]=geografia_total[('registros Continentales EN_IUCN')].fillna(value=0)+geografia_total[('registros Continentales CR_IUCN')].fillna(value=0)+geografia_total[('registros Continentales VU_IUCN')].fillna(value=0)
        geografia_total[('especies_salobres_amenazadas_global_total')]=geografia_total[('especies Salobres EN_IUCN')].fillna(value=0)+geografia_total[('especies Salobres CR_IUCN')].fillna(value=0)+geografia_total[('especies Salobres VU_IUCN')].fillna(value=0)
        geografia_total[('registros_salobres_amenazadas_global_total')]=geografia_total[('registros Salobres EN_IUCN')].fillna(value=0)+geografia_total[('registros Salobres CR_IUCN')].fillna(value=0)+geografia_total[('registros Salobres VU_IUCN')].fillna(value=0)
    
        geografia_total[('especies_marinas_amenazadas_global_total')]=geografia_total[('especies_marinas_amenazadas_global_total')].replace(0,'',regex=True)
        geografia_total[('registros_marinas_amenazadas_global_total')]=geografia_total[('registros_marinas_amenazadas_global_total')].replace(0,'',regex=True)
        geografia_total[('especies_continentales_amenazadas_global_total')]=geografia_total[('especies_continentales_amenazadas_global_total')].replace(0,'',regex=True)
        geografia_total[('registros_continentales_amenazadas_global_total')]=geografia_total[('registros_continentales_amenazadas_global_total')].replace(0,'',regex=True)
        geografia_total[('especies_salobres_amenazadas_global_total')]=geografia_total[('especies_salobres_amenazadas_global_total')].replace(0,'',regex=True)
        geografia_total[('registros_salobres_amenazadas_global_total')]=geografia_total[('registros_salobres_amenazadas_global_total')].replace(0,'',regex=True)
    
        print(geografia_total['especies_continentales_exoticas_total'].values)
        print(geografia_total['especies_continentales_exoticas_total'].unique)

    if region=='slug_x' or region=='slug_col':     
        if tipo =='DCDM' or tipo =='DSDM':
            geografia_total=pd.merge(geografia_total,estimadas_dept,left_on='slug_x',right_on='departamento',how='left')
            geografia_total['estimada_region_ref_id']='87'
        if tipo =='CCDM' or tipo =='CSDM':
            geografia_total=pd.merge(geografia_total,estimadas_dept,left_on='slug_col',right_on='departamento',how='left')
            geografia_total['estimada_region_ref_id']='86'
        
    geografia_total['fecha_corte']=fecha_corte
    geografia_total=geografia_total.replace(np.nan,'',regex=True)
    
    ##---------------------14. Transformación cifras por categoría taxonómica a cifras por grupos biológicos----------------------------##
    '''
    Se carga el archivo guía de grupos biologicos y con este se calculan las cifras tematicas para cada uno de estos
    Dentro de los nombres se encuentran las siguientes convenciones:
    rb= Registros biologicos
    sp= especies
    '''

    ## Resumen cifras registros por grupo biológico general
    rb_grupos_biologicos=pd.merge(rb_taxon_total,grupos_biologicos, on=['grupoTax','taxonRank'],how='left').drop_duplicates() 
    
    del rb_taxon_total
    if tipo =='CCDM':
        ##Resumen cifras registros por grupo biológico para marinos, continentales y salobres
        rb_grupos_biologicos=rb_grupos_biologicos[['registros','registrosContinentales','registrosMarinos','registrosSalobres','grupoTax','grupo_id','tipo_grupo']]
    
    if tipo =='MCDM' or tipo =='DCDM':
        ##Resumen cifras registros por grupo biológico para marinos, continentales y salobres
        rb_grupos_biologicos=rb_grupos_biologicos[['registros',region,'registrosContinentales','registrosMarinos','registrosSalobres','grupoTax','grupo_id','tipo_grupo']]
    
    if tipo =='CSDM':
        ## Seleccionar las columnas deseadas 
        rb_grupos_biologicos=rb_grupos_biologicos[['registros','grupoTax','grupo_id','tipo_grupo']]
    
    if tipo =='MSDM' or tipo =='DSDM':
        ## Seleccionar las columnas deseadas 
        rb_grupos_biologicos=rb_grupos_biologicos[['registros',region,'grupoTax','grupo_id','tipo_grupo']]
    
    if tipo =='CCDM' or tipo =='CSDM':
        ## Agrupar registros por grupo biológico
        rb_cifras=rb_grupos_biologicos.groupby(['grupo_id','tipo_grupo']).sum().reset_index()
        
    if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
        ## Agrupar registros por grupo biológico
        rb_cifras=rb_grupos_biologicos.groupby(['grupo_id',region,'tipo_grupo']).sum().reset_index()
    
    del rb_grupos_biologicos
    ## Resumen cifras especies por grupo biológico general
    sp_grupos_biologicos=pd.merge(sp_taxon_total,grupos_biologicos, on=['grupoTax','taxonRank'],how='left').drop_duplicates()
    del sp_taxon_total
    ## Condicional para registros marinos, continentales y salobres
    if tipo =='CCDM':
        ##Resumen cifras especies por grupo biológico para marinos, continentales y salobres
        sp_grupos_biologicos=sp_grupos_biologicos[['especies','especiesMarinas','especiesContinentales','especiesSalobres','grupoTax','grupo_id']]
    if tipo =='CSDM':
        ##Seleccionar las columnas deseadas y agrupar por grupo biológico
        sp_grupos_biologicos=sp_grupos_biologicos[['especies','grupoTax','grupo_id']]
    
    if tipo =='MCDM' or tipo =='DCDM':
        ##Resumen cifras especies por grupo biológico para marinos, continentales y salobres
        sp_grupos_biologicos=sp_grupos_biologicos[['especies',region,'especiesMarinas','especiesContinentales','especiesSalobres','grupoTax','grupo_id']]
    if tipo =='MSDM' or tipo =='DSDM': 
        ##Seleccionar las columnas deseadas y agrupar por grupo biológico
        sp_grupos_biologicos=sp_grupos_biologicos[['especies',region,'grupoTax','grupo_id']]
    
    if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
        sp_cifras=sp_grupos_biologicos.groupby(['grupo_id',region]).sum().reset_index()
        
        ## Unión de las cifras de registros y especies para cada grupo biológico
        taxon_cifras=pd.merge(rb_cifras,sp_cifras, on=['grupo_id',region],how='left') 
    
    if tipo =='CCDM' or tipo =='CSDM':
        sp_cifras=sp_grupos_biologicos.groupby('grupo_id').sum().reset_index()
    
        ## Unión de las cifras de registros y especies para cada grupo biológico
        taxon_cifras=pd.merge(rb_cifras,sp_cifras, on=['grupo_id'],how='left') 
    
    ## Resumen cifras por grupo biológico y temática general
    taxon_tematica_cifras=pd.merge(taxon_tematica,grupos_biologicos, on=['grupoTax','taxonRank'],how='left').drop_duplicates()
    del taxon_tematica
    if tipo =='MSDM' or tipo =='DSDM':
        ## Seleccionar las columnas deseadas y agrupar por grupo biológico y temática
        taxon_tematica_cifras=taxon_tematica_cifras[['registros','especies','grupo_id','thematic',region]]
    ## Condicional para registros marinos, continentales y salobres
    if tipo =='MCDM' or tipo =='DCDM':
        ##Resumen cifras por grupo biológico y temática para marinos, continentales y salobres
        taxon_tematica_cifras=taxon_tematica_cifras[['registros','registrosContinentales','registrosMarinos','registrosSalobres','especies','especiesMarinas','especiesContinentales','especiesSalobres','grupo_id','thematic',region]]
    ## Condicional para registros marinos, continentales y salobres
    if tipo =='CCDM':
        ##Resumen cifras por grupo biológico y temática para marinos, continentales y salobres
        taxon_tematica_cifras=taxon_tematica_cifras[['registros','registrosContinentales','registrosMarinos','registrosSalobres','especies','especiesMarinas','especiesContinentales','especiesSalobres','grupo_id','thematic']]
    if tipo =='CSDM':
        ## Seleccionar las columnas deseadas y agrupar por grupo biológico y temática
        taxon_tematica_cifras=taxon_tematica_cifras[['registros','especies','grupo_id','thematic']]
    
    
    if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
        ## Agrupar las cifras por grupo biológico y temática, y reordenar el conjunto de datos
        taxon_tematica_cifras=taxon_tematica_cifras.groupby(['grupo_id','thematic',region]).sum().reset_index()
#        taxon_tematica_cifras=taxon_tematica_cifras.pivot_table(index=['grupo_id',region], columns=['thematic'])
#        taxon_tematica_cifras=taxon_tematica_cifras.pivot_table(index=['grupo_id',region], columns=['thematic']).reset_index()
        taxon_tematica_cifras = pd.pivot_table(taxon_tematica_cifras, values=["especies","registros"], index=['grupo_id',region], columns=['thematic'])
#        taxon_tematica_cifras.columns=taxon_tematica_cifras.columns.map(" ".join)
        # Aplanar columnas correctamente
        taxon_tematica_cifras.columns = (
            taxon_tematica_cifras.columns
                .to_flat_index()
                .map(lambda x: " ".join(map(str, x)).strip())
        )
        
        taxon_tematica_cifras = taxon_tematica_cifras.reset_index()
                
    if tipo =='CCDM' or tipo =='CSDM':
        ## Agrupar las cifras por grupo biológico y temática, y reordenar el conjunto de datos
        taxon_tematica_cifras=taxon_tematica_cifras.groupby(['grupo_id','thematic']).sum().reset_index()
#       taxon_tematica_cifras=taxon_tematica_cifras.pivot_table('grupo_id','thematic')#.fillna('-') 
        taxon_tematica_cifras = pd.pivot_table(taxon_tematica_cifras, values=["especies","registros"], index="grupo_id",columns=["thematic"])
        taxon_tematica_cifras.columns=taxon_tematica_cifras.columns.map(" ".join)   
        taxon_tematica_cifras=taxon_tematica_cifras.add_prefix('especies ')
        
    ## Resumen cifras de especies y registros por grupo biológico y por categoría dentro de cada temática general
    taxon_categoria_cifras=pd.merge(taxon_categoria,grupos_biologicos, on=['grupoTax','taxonRank'],how='left').drop_duplicates()
    
    del taxon_categoria
    
    ##Condicional para registros marinos, continentales y salobres
    if tipo =='CCDM':
        #Resumen cifras de especies y registros por grupo biológico y por categoría dentro de cada temática para marinos, continentales y salobres
        taxon_categoria_cifras=taxon_categoria_cifras[['registros','registrosContinentales','registrosMarinos','registrosSalobres','especies','especiesMarinas','especiesContinentales','especiesSalobres','grupo_id','thematic','category']]
    if tipo =='CSDM':
        ##Seleccionar las columnas deseadas 
        taxon_categoria_cifras=taxon_categoria_cifras[['registros','especies','grupo_id','thematic','category']]
    if tipo =='MSDM' or tipo =='DSDM':
        ##Seleccionar las columnas deseadas 
        taxon_categoria_cifras=taxon_categoria_cifras[['registros','especies',region,'grupo_id','thematic','category']]
    if tipo =='MCDM' or tipo =='DCDM':
        taxon_categoria_cifras=taxon_categoria_cifras[['registros','registrosContinentales','registrosMarinos','registrosSalobres','especies','especiesMarinas','especiesContinentales','especiesSalobres','grupo_id','thematic','category',region]]
    
    
    if tipo =='MSDM' or tipo =='DSDM' or tipo =='MCDM' or tipo =='DCDM':
        ## Agrupar por grupo biológico y temática, eliminar la columna temática y reordenar el conjunto de datos   
        taxon_categoria_cifras=taxon_categoria_cifras.groupby(['grupo_id','thematic','category',region]).sum().reset_index()
        taxon_categoria_cifras=taxon_categoria_cifras.drop(['thematic'],axis=1)#.pivot(index=['grupo_id',region], columns=['category'])
#        taxon_categoria_cifras = pd.pivot_table(taxon_categoria_cifras, values='especies', index=region,columns=["category"])
#        taxon_categoria_cifras = pd.pivot_table(taxon_categoria_cifras, values='especies', index=['grupo_id', region],columns=["category"]).reset_index()
        taxon_categoria_cifras = pd.pivot_table(taxon_categoria_cifras, values=["especies","registros"], index=['grupo_id', region],columns=["category"])
#        taxon_categoria_cifras.columns = taxon_categoria_cifras.columns.str.strip()
#        taxon_categoria_cifras.columns=taxon_categoria_cifras.columns.map(" ".join)
#        taxon_categoria_cifras=taxon_categoria_cifras.add_prefix('especies ')

        # Aplanar columnas correctamente
        taxon_categoria_cifras.columns = (
            taxon_categoria_cifras.columns
                .to_flat_index()
                .map(lambda x: " ".join(map(str, x)).strip())
        )
        
        taxon_categoria_cifras = taxon_categoria_cifras.reset_index()
        
        
        ##Unión de las cifras de especies y registros para los dos grupos biológicos
        grupos_biologicos_total=pd.merge(taxon_cifras,taxon_tematica_cifras,on=['grupo_id',region],how='left').merge(taxon_categoria_cifras,on=['grupo_id',region],how='left') ###FinalBiologicalGroupFile
    
    if tipo =='CCDM' or tipo =='CSDM':
        ## Agrupar por grupo biológico y temática, eliminar la columna temática y reordenar el conjunto de datos   
        taxon_categoria_cifras=taxon_categoria_cifras.groupby(['grupo_id','thematic','category']).sum().reset_index()
        taxon_categoria_cifras=taxon_categoria_cifras.drop(['thematic'],axis=1)#.pivot('grupo_id','category')
        taxon_categoria_cifras = pd.pivot_table(taxon_categoria_cifras, values=["especies","registros"], index="grupo_id",columns=["category"])        
        taxon_categoria_cifras.columns=taxon_categoria_cifras.columns.map(" ".join)
#        taxon_categoria_cifras=taxon_categoria_cifras.add_prefix('especies ')
        ##Unión de las cifras de especies y registros para los dos grupos biológicos
        grupos_biologicos_total=pd.merge(taxon_cifras,taxon_tematica_cifras,on='grupo_id',how='left').merge(taxon_categoria_cifras,on='grupo_id',how='left') ###FinalBiologicalGroupFile
        grupos_biologicos_total['slug_col']=slug_region
    print('---)'+str(grupos_biologicos_total.columns))
    del taxon_tematica_cifras
    print("==== COLUMNAS DISPONIBLES ====")
    print(taxon_categoria_cifras.columns)
    del taxon_categoria_cifras
    #print(grupos_biologicos_total.columns)
    if ('especies Exótica') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica')]=0
    if ('especies Invasora') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Invasora')]=0
    if ('especies Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica con potencial de invasión')]=0
    if ('especies Trasplantada') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Trasplantada')]=0   
        
    if ('especies Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica con potencial de invasión Alto Riesgo')]=0
    if ('especies Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica con potencial de invasión Bajo Riesgo')]=0
    if ('especies Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado')]=0
    if ('especies Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

        
    if ('especies EN_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies EN_IUCN')]=0
    if ('especies CR_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies CR_IUCN')]=0
    if ('especies VU_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('especies VU_IUCN')]=0
    
    if ('registros Exótica') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica')]=0
    if ('registros Invasora') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Invasora')]=0
    if ('registros Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica con potencial de invasión')]=0
    if ('registros Trasplantada') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Trasplantada')]=0    

    if ('registros Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica con potencial de invasión Alto Riesgo')]=0
    if ('registros Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica con potencial de invasión Bajo Riesgo')]=0
    if ('registros Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado')]=0
    if ('registros Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

        
    if ('registros EN_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros EN_IUCN')]=0
    if ('registros CR_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros CR_IUCN')]=0
    if ('registros VU_IUCN') not in grupos_biologicos_total.columns:
        grupos_biologicos_total[('registros VU_IUCN')]=0
  
    grupos_biologicos_total['especies_exoticas_total']=grupos_biologicos_total[('especies Exótica')].fillna(value=0)+grupos_biologicos_total[('especies Invasora')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)+grupos_biologicos_total[('especies Trasplantada')].fillna(value=0)
    grupos_biologicos_total['registros_exoticas_total']=grupos_biologicos_total[('registros Exótica')].fillna(value=0)+grupos_biologicos_total[('registros Invasora')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)+grupos_biologicos_total[('registros Trasplantada')].fillna(value=0)
    grupos_biologicos_total['especies_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('especies Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('especies Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
    grupos_biologicos_total['registros_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('registros Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('registros Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        
    
    grupos_biologicos_total[('especies_amenazadas_global_total')]=pd.to_numeric(grupos_biologicos_total[('especies EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies VU_IUCN')].fillna(value=0))
    grupos_biologicos_total[('registros_amenazadas_global_total')]=pd.to_numeric(grupos_biologicos_total[('registros EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros VU_IUCN')].fillna(value=0))
    grupos_biologicos_total[('especies_exoticas_total')]=grupos_biologicos_total[('especies_exoticas_total')].replace(0,'',regex=True)
    grupos_biologicos_total[('registros_exoticas_total')]=grupos_biologicos_total[('registros_exoticas_total')].replace(0,'',regex=True)
    grupos_biologicos_total[('especies_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('especies_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    grupos_biologicos_total[('registros_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('registros_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    grupos_biologicos_total[('especies_amenazadas_global_total')]=grupos_biologicos_total[('especies_amenazadas_global_total')].replace(0,'',regex=True)
    grupos_biologicos_total[('registros_amenazadas_global_total')]=grupos_biologicos_total[('registros_amenazadas_global_total')].replace(0,'',regex=True)
    
    if tipo =='MCDM' or tipo =='DCDM' or tipo =='CCDM':
        if ('especies Continentales Exóticas') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica')]=0
        if ('especies Continentales Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Invasora')]=0
        if ('especies Continentales Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión')]=0
        if ('especies Continentales Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Trasplantada')]=0        
            
        if ('especies Continentales Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Continentales Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Continentales Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('especies Continentales EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales EN_IUCN')]=0
        if ('especies Continentales CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales CR_IUCN')]=0
        if ('especies Continentales VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Continentales VU_IUCN')]=0
        
        if ('registros Continentales Exótica') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica')]=0
        if ('registros Continentales Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Invasora')]=0
        if ('registros Continentales Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión')]=0
        if ('registros Continentales Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Trasplantada')]=0    

        if ('registros Continentales Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Continentales Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Continentales Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('registros Continentales EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales EN_IUCN')]=0
        if ('registros Continentales CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales CR_IUCN')]=0
        if ('registros Continentales VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Continentales VU_IUCN')]=0
            
        if ('especies Marinas Exótica') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica')]=0
        if ('especies Marinas Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Invasora')]=0
        if ('especies Marinas Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión')]=0
        if ('especies Marinas Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Trasplantada')]=0    
            
        if ('especies Marinas Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Marinas Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Marinas Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0
            
        if ('especies Marinas EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas EN_IUCN')]=0
        if ('especies Marinas CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas CR_IUCN')]=0
        if ('especies Marinas VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Marinas VU_IUCN')]=0
       
        if ('registros Marinos Exótica') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica')]=0
        if ('registros Marinos Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Invasora')]=0
        if ('registros Marinos Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión')]=0
        if ('registros Marinos Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Trasplantada')]=0     
            
        if ('registros Marinos Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Marinos Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Marinos Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

        
        if ('registros Marinos EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos EN_IUCN')]=0
        if ('registros Marinos CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos CR_IUCN')]=0
        if ('registros Marinos VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Marinos VU_IUCN')]=0
            
        if ('especies Salobres Exótica') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica')]=0
        if ('especies Salobres Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Invasora')]=0
        if ('especies Salobres Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión')]=0
        if ('especies Salobres Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Trasplantada')]=0 
            
        if ('especies Salobres Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Alto Riesgo')]=0
        if ('especies Salobres Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('especies Salobres Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('especies Salobres EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres EN_IUCN')]=0
        if ('especies Salobres CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres CR_IUCN')]=0
        if ('especies Salobres VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('especies Salobres VU_IUCN')]=0
        
        if ('registros Salobres Exótica') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica')]=0
        if ('registros Salobres Invasora') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Invasora')]=0
        if ('registros Salobres Exótica con potencial de invasión') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión')]=0
        if ('registros Salobres Trasplantada') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Trasplantada')]=0      

        if ('registros Salobres Exótica con potencial de invasión Alto Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Alto Riesgo')]=0
        if ('registros Salobres Exótica con potencial de invasión Bajo Riesgo') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Bajo Riesgo')]=0
        if ('registros Salobres Exótica con potencial de invasión Riesgo Moderado') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado')]=0
        if ('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')]=0

            
        if ('registros Salobres EN_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres EN_IUCN')]=0
        if ('registros Salobres CR_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres CR_IUCN')]=0
        if ('registros Salobres VU_IUCN') not in grupos_biologicos_total.columns:
            grupos_biologicos_total[('registros Salobres VU_IUCN')]=0
 
        grupos_biologicos_total['especies_continentales_exoticas_total']=grupos_biologicos_total[('especies Continentales Exótica')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Invasora')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Trasplantada')].fillna(value=0)
        grupos_biologicos_total['registros_continentales_exoticas_total']=grupos_biologicos_total[('registros Continentales Exótica')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Invasora')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Trasplantada')].fillna(value=0)
        grupos_biologicos_total['especies_marinas_exoticas_total']=grupos_biologicos_total[('especies Marinas Exótica')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Invasora')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Trasplantada')].fillna(value=0)
        grupos_biologicos_total['registros_marinas_exoticas_total']=grupos_biologicos_total[('registros Marinos Exótica')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Invasora')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Trasplantada')].fillna(value=0)
        grupos_biologicos_total['especies_salobres_exoticas_total']=grupos_biologicos_total[('especies Salobres Exótica')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Invasora')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Trasplantada')].fillna(value=0)
        grupos_biologicos_total['registros_salobres_exoticas_total']=grupos_biologicos_total[('registros Salobres Exótica')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Invasora')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Trasplantada')].fillna(value=0)

        grupos_biologicos_total['especies_continentales_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        grupos_biologicos_total['registros_continentales_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        grupos_biologicos_total['especies_marinas_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        grupos_biologicos_total['registros_marinas_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        grupos_biologicos_total['especies_salobres_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
        grupos_biologicos_total['registros_salobres_exoticas_riesgo_invasion_total']=grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Alto Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Bajo Riesgo')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado')].fillna(value=0)+grupos_biologicos_total[('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto')].fillna(value=0)
    
    
        grupos_biologicos_total[('especies_marinas_amenazadas_global_total')]=grupos_biologicos_total[('especies Marinas EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Marinas CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Marinas VU_IUCN')].fillna(value=0)
        grupos_biologicos_total[('registros_marinas_amenazadas_global_total')]=grupos_biologicos_total[('registros Marinos EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Marinos CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Marinos VU_IUCN')].fillna(value=0)
        grupos_biologicos_total[('especies_continentales_amenazadas_global_total')]=grupos_biologicos_total[('especies Continentales EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Continentales CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Continentales VU_IUCN')].fillna(value=0)
        grupos_biologicos_total[('registros_continentales_amenazadas_global_total')]=grupos_biologicos_total[('registros Continentales EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Continentales CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Continentales VU_IUCN')].fillna(value=0)
        grupos_biologicos_total[('especies_salobres_amenazadas_global_total')]=grupos_biologicos_total[('especies Salobres EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Salobres CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('especies Salobres VU_IUCN')].fillna(value=0)
        grupos_biologicos_total[('registros_salobres_amenazadas_global_total')]=grupos_biologicos_total[('registros Salobres EN_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Salobres CR_IUCN')].fillna(value=0)+grupos_biologicos_total[('registros Salobres VU_IUCN')].fillna(value=0)
    

        grupos_biologicos_total[('especies_continentales_exoticas_total')]=grupos_biologicos_total[('especies_continentales_exoticas_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_continentales_exoticas_total')]=grupos_biologicos_total[('registros_continentales_exoticas_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_marinas_exoticas_total')]=grupos_biologicos_total[('especies_marinas_exoticas_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_marinas_exoticas_total')]=grupos_biologicos_total[('registros_marinas_exoticas_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_salobres_exoticas_total')]=grupos_biologicos_total[('especies_salobres_exoticas_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_salobres_exoticas_total')]=grupos_biologicos_total[('registros_salobres_exoticas_total')].replace(0,'',regex=True)
        
        grupos_biologicos_total[('especies_continentales_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('especies_continentales_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_continentales_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('registros_continentales_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_marinas_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('especies_marinas_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_marinas_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('registros_marinas_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_salobres_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('especies_salobres_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_salobres_exoticas_riesgo_invasion_total')]=grupos_biologicos_total[('registros_salobres_exoticas_riesgo_invasion_total')].replace(0,'',regex=True)
    
        grupos_biologicos_total[('especies_marinas_amenazadas_global_total')]=grupos_biologicos_total[('especies_marinas_amenazadas_global_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_marinas_amenazadas_global_total')]=grupos_biologicos_total[('registros_marinas_amenazadas_global_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_continentales_amenazadas_global_total')]=grupos_biologicos_total[('especies_continentales_amenazadas_global_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_continentales_amenazadas_global_total')]=grupos_biologicos_total[('registros_continentales_amenazadas_global_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('especies_salobres_amenazadas_global_total')]=grupos_biologicos_total[('especies_salobres_amenazadas_global_total')].replace(0,'',regex=True)
        grupos_biologicos_total[('registros_salobres_amenazadas_global_total')]=grupos_biologicos_total[('registros_salobres_amenazadas_global_total')].replace(0,'',regex=True)
    
    grupos_biologicos_total=grupos_biologicos_total.replace(np.nan,'',regex=True)
#    print(geografia_total.columns)
#    print(grupos_biologicos_total.columns) 


    ##-----------------------15. Renombrar y reorganizar las columnas del dataframe de cifras por grupo biológico y geograficas-----------------##
    '''
    En esta sección se crea una función para renombrar y reorganizar los conjuntos de datos obtenidos para grupos biológicos y cifras geográficas.
    Según la información deseada y los campos que se requieran se debe ajustar el diccionario 'names'.
    El primera campo corresponde al nombre que tiene la columna en el conjunto de datos y el segundo al nombre que desea que tenga.}
    Los nombres que no se encuentren dentro del diccionario pero estan en el conjunto de datos inical no quedaran en el archivo final.
    Si se desea modificar el orden de los campos, solo se debe ajustar el orden dentro del diccionario. 
    Esta función cuenta con dos parametros de entrada: 
    archivo: corresponde al conjunto de datos grupo biológico:grupos_biologicos_total cifras geográficas: geografia_total
    tipo: Corresponde a un parametro númerico donde 1: grupo biológico y 2: cifras geográficas. Este parametro permite a la función deteminar el valor
    de la primera columna (grupoBio o (county o stateProvince))
    '''
    ##Creación de la función con dos parametros de entrada
    
    print("ANTES DE AJUSTE:")
    print(grupos_biologicos_total.columns)
    
    def ajuste_nombres (archivo, tipo):
    
        ## Para grupos biologicos: Si el valor de tipo es 1 asigna el valor de 'grupoBio' a las variables llave y valor
        if tipo== 1:
            llave='grupo_id'
            valor='slug_grupo'
    
        ## Para cifras geograficas: Si el valor de tipo es 2 asigna el valor de 'county o stateProvince' a las variables llave y valor    
        if tipo==2:
            llave=region
            valor=region
    
    
        ##Diccionario de datos con los nombres provenientes del conjunto de datos y su correspondiente valor ajustado para exportar 
        names= {llave:valor, 
        region:'slug_region',
        'tipo_grupo':'tipo',
        'fecha_corte':'fecha_corte',
        'estimada':'especies_region_estimadas',
        'estimada_region_ref_id':'estimada_region_ref_id',
        'registros':'registros_region_total', 
        'registrosContinentales':'registros_continentales', 
        'registrosMarinos':'registros_marinos', 
        'registrosSalobres':'registros_salobres', 
        'especies':'especies_region_total', 
        'especiesContinentales':'especies_continentales', 
        'especiesMarinas':'especies_marinas', 
        'especiesSalobres':'especies_salobres',           
        ('especies threatStatus_MADS'):'especies_amenazadas_nacional_total', 
        ('especies CR_MADS'):'especies_amenazadas_nacional_cr',
        ('especies EN_MADS'):'especies_amenazadas_nacional_en', 
        ('especies VU_MADS'):'especies_amenazadas_nacional_vu',
        ('registros threatStatus_MADS'):'registros_amenazadas_nacional_total', 
        ('registros CR_MADS'):'registros_amenazadas_nacional_cr', 
        ('registros EN_MADS'):'registros_amenazadas_nacional_en',
        ('registros VU_MADS'):'registros_amenazadas_nacional_vu',  
        ('especies appendixCITES'):'especies_cites_total', 
        ('especies I'):'especies_cites_i', 
        ('especies I/II'):'especies_cites_i_ii',
        ('especies II'):'especies_cites_ii', 
        ('especies III'):'especies_cites_iii', 
        ('registros appendixCITES'):'registros_cites_total', 
        ('registros I'):'registros_cites_i', 
        ('registros I/II'):'registros_cites_i_ii',
        ('registros II'):'registros_cites_ii', 
        ('registros III'):'registros_cites_iii', 
        'especies_exoticas_total':'especies_exoticas_total', 
        ('especies Exótica'):'especies_exoticas', 
        ('especies Invasora'):'especies_invasoras',
        'especies_exoticas_riesgo_invasion_total':'especies_exoticas_riesgo_invasion_total',        
        ('especies Exótica con potencial de invasión Alto Riesgo'):'especies_exoticas_riesgo_invasion_alto',
        ('especies Exótica con potencial de invasión Bajo Riesgo'):'especies_exoticas_riesgo_invasion_bajo', 
        ('especies Exótica con potencial de invasión Riesgo Moderado'):'especies_exoticas_riesgo_invasion_moderado',  
        ('especies Exótica con potencial de invasión Riesgo Moderado/ Alto'):'especies_exoticas_riesgo_invasion_moderado_alto',          
        ('especies Trasplantada'):'especies_trasplantadas',
        'registros_exoticas_total':'registros_exoticas_total',
        ('registros Exótica'):'registros_exoticas', 
        ('registros Invasora'):'registros_invasoras',
        'registros_exoticas_riesgo_invasion_total':'registros_exoticas_riesgo_invasion_total',
        ('registros Exótica con potencial de invasión Alto Riesgo'):'registros_exoticas_riesgo_invasion_alto',        
        ('registros Exótica con potencial de invasión Bajo Riesgo'):'registros_exoticas_riesgo_invasion_bajo',        
        ('registros Exótica con potencial de invasión Riesgo Moderado'):'registros_exoticas_riesgo_invasion_moderado',        
        ('registros Exótica con potencial de invasión Riesgo Moderado/ Alto'):'registros_exoticas_riesgo_invasion_moderado_alto',        
        ('registros Trasplantada'):'registros_trasplantadas',
        ('especies endemic'):'especies_endemicas', 
        ('especies migratory'):'especies_migratorias', 
        ('registros endemic'):'registros_endemicas',     
        ('registros migratory'):'registros_migratorias', 
        'especies_amenazadas_global_total':'especies_amenazadas_global_total',
        #('especies', 'threatStatus_UICN'):'especies_amenazadas_global_total',  
        ('especies EX_IUCN'):'especies_amenazadas_global_ex', 
        ('especies EW_IUCN'):'especies_amenazadas_global_ew', 
        ('especies CR_IUCN'):'especies_amenazadas_global_cr', 
        ('especies EN_IUCN'):'especies_amenazadas_global_en', 
        ('especies VU_IUCN'):'especies_amenazadas_global_vu',
        ('especies NT_IUCN'):'especies_amenazadas_global_nt', 
        ('especies LC_IUCN'):'especies_amenazadas_global_lc', 
        ('especies DD_IUCN'):'especies_amenazadas_global_dd',
        ('especies LR/lc_IUCN'):'especies_amenazadas_global_lr_lc', 
        ('especies LR/nt_IUCN'):'especies_amenazadas_global_lr_nt',
        'registros_amenazadas_global_total':'registros_amenazadas_global_total',
        ('registros EX_IUCN'):'registros_amenazadas_global_ex', 
        ('registros EW_IUCN'):'registros_amenazadas_global_ew', 
        ('registros CR_IUCN'):'registros_amenazadas_global_cr', 
        ('registros EN_IUCN'):'registros_amenazadas_global_en', 
        ('registros VU_IUCN'):'registros_amenazadas_global_vu', 
        ('registros NT_IUCN'):'registros_amenazadas_global_nt', 
        ('registros LC_IUCN'):'registros_amenazadas_global_lc',
        ('registros DD_IUCN'):'registros_amenazadas_global_dd',    
        ('registros LR/lc_IUCN'):'registros_amenazadas_global_lr_lc', 
        ('registros LR/nt_IUCN'):'registros_amenazadas_global_lr_nt', 
        
        
        ('especies Continentales threatStatus_MADS'):'especies_continentales_amenazadas_nacional_total', 
        ('especies Continentales CR_MADS'):'especies_continentales_amenazadas_nacional_cr',
        ('especies Continentales EN_MADS'):'especies_continentales_amenazadas_nacional_en', 
        ('especies Continentales VU_MADS'):'especies_continentales_amenazadas_nacional_vu',
        ('registros Continentales threatStatus_MADS'):'registros_continentales_amenazadas_nacional_total', 
        ('registros Continentales CR_MADS'):'registros_continentales_amenazadas_nacional_cr', 
        ('registros Continentales EN_MADS'):'registros_continentales_amenazadas_nacional_en',
        ('registros Continentales VU_MADS'):'registros_continentales_amenazadas_nacional_vu',  
        ('especies Continentales appendixCITES'):'especies_continentales_cites_total', 
        ('especies Continentales I'):'especies_continentales_cites_i', 
        ('especies Continentales I/II'):'especies_continentales_cites_i_ii',
        ('especies Continentales II'):'especies_continentales_cites_ii', 
        ('especies Continentales III'):'especies_continentales_cites_iii', 
        ('registros Continentales appendixCITES'):'registros_continentales_cites_total', 
        ('registros Continentales I'):'registros_continentales_cites_i', 
        ('registros Continentales I/II'):'registros_continentales_cites_i_ii',
        ('registros Continentales II'):'registros_continentales_cites_ii', 
        ('registros Continentales III'):'registros_continentales_cites_iii', 
        'especies_continentales_exoticas_total':'especies_continentales_exoticas_total', 
        ('especies Continentales Exótica'):'especies_continentales_exoticas', 
        ('especies Continentales Invasora'):'especies_continentales_invasoras',
        'especies_continentales_exoticas_riesgo_invasion_total':'especies_continentales_exoticas_riesgo_invasion_total',        
        ('especies Continentales Exótica con potencial de invasión Alto Riesgo'):'especies_continentales_exoticas_riesgo_invasion_alto',
        ('especies Continentales Exótica con potencial de invasión Bajo Riesgo'):'especies_continentales_exoticas_riesgo_invasion_bajo', 
        ('especies Continentales Exótica con potencial de invasión Riesgo Moderado'):'especies_continentales_exoticas_riesgo_invasion_moderado',  
        ('especies Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto'):'especies_continentales_exoticas_riesgo_invasion_moderado_alto',          
        ('especies Continentales Trasplantada'):'especies_continentales_trasplantadas',
        'registros_continentales_exoticas_total':'registros_continentales_exoticas_total',
        ('registros Continentales Exótica'):'registros_continentales_exoticas', 
        ('registros Continentales Invasora'):'registros_continentales_invasoras',
        'registros_continentales_exoticas_riesgo_invasion_total':'registros_continentales_exoticas_riesgo_invasion_total',
        ('registros Continentales Exótica con potencial de invasión Alto Riesgo'):'registros_continentales_exoticas_riesgo_invasion_alto',        
        ('registros Continentales Exótica con potencial de invasión Bajo Riesgo'):'registros_continentales_exoticas_riesgo_invasion_bajo',        
        ('registros Continentales Exótica con potencial de invasión Riesgo Moderado'):'registros_continentales_exoticas_riesgo_invasion_moderado',        
        ('registros Continentales Exótica con potencial de invasión Riesgo Moderado/ Alto'):'registros_continentales_exoticas_riesgo_invasion_moderado_alto',        
        ('registros Continentales Trasplantada'):'registros_continentales_trasplantadas',
        ('especies Continentales endemic'):'especies_continentales_endemicas', 
        ('especies Continentales migratory'):'especies_continentales_migratorias', 
        ('registros Continentales endemic'):'registros_continentales_endemicas',     
        ('registros Continentales migratory'):'registros_continentales_migratorias', 
        'especies_continentales_amenazadas_global_total':'especies_continentales_amenazadas_global_total',
        #('especies', 'threatStatus_UICN'):'especies_continentales_amenazadas_global_total',  
        ('especies Continentales EX_IUCN'):'especies_continentales_amenazadas_global_ex', 
        ('especies Continentales EW_IUCN'):'especies_continentales_amenazadas_global_ew', 
        ('especies Continentales CR_IUCN'):'especies_continentales_amenazadas_global_cr', 
        ('especies Continentales EN_IUCN'):'especies_continentales_amenazadas_global_en', 
        ('especies Continentales VU_IUCN'):'especies_continentales_amenazadas_global_vu',
        ('especies Continentales NT_IUCN'):'especies_continentales_amenazadas_global_nt', 
        ('especies Continentales LC_IUCN'):'especies_continentales_amenazadas_global_lc', 
        ('especies Continentales DD_IUCN'):'especies_continentales_amenazadas_global_dd',
        ('especies Continentales LR/lc_IUCN'):'especies_continentales_amenazadas_global_lr_lc', 
        ('especies Continentales LR/nt_IUCN'):'especies_continentales_amenazadas_global_lr_nt',
        'registros_continentales_amenazadas_global_total':'registros_continentales_amenazadas_global_total',
        ('registros Continentales EX_IUCN'):'registros_continentales_amenazadas_global_ex', 
        ('registros Continentales EW_IUCN'):'registros_continentales_amenazadas_global_ew', 
        ('registros Continentales CR_IUCN'):'registros_continentales_amenazadas_global_cr', 
        ('registros Continentales EN_IUCN'):'registros_continentales_amenazadas_global_en', 
        ('registros Continentales VU_IUCN'):'registros_continentales_amenazadas_global_vu', 
        ('registros Continentales NT_IUCN'):'registros_continentales_amenazadas_global_nt', 
        ('registros Continentales LC_IUCN'):'registros_continentales_amenazadas_global_lc',
        ('registros Continentales DD_IUCN'):'registros_continentales_amenazadas_global_dd',    
        ('registros Continentales LR/lc_IUCN'):'registros_continentales_amenazadas_global_lr_lc', 
        ('registros Continentales LR/nt_IUCN'):'registros_continentales_amenazadas_global_lr_nt',


        ('especies Marinas threatStatus_MADS'):'especies_marinas_amenazadas_nacional_total', 
        ('especies Marinas CR_MADS'):'especies_marinas_amenazadas_nacional_cr',
        ('especies Marinas EN_MADS'):'especies_marinas_amenazadas_nacional_en', 
        ('especies Marinas VU_MADS'):'especies_marinas_amenazadas_nacional_vu',
        ('registros Marinos threatStatus_MADS'):'registros_marinas_amenazadas_nacional_total', 
        ('registros Marinos CR_MADS'):'registros_marinas_amenazadas_nacional_cr', 
        ('registros Marinos EN_MADS'):'registros_marinas_amenazadas_nacional_en',
        ('registros Marinos VU_MADS'):'registros_marinas_amenazadas_nacional_vu',  
        ('especies Marinas appendixCITES'):'especies_marinas_cites_total', 
        ('especies Marinas I'):'especies_marinas_cites_i', 
        ('especies Marinas I/II'):'especies_marinas_cites_i_ii',
        ('especies Marinas II'):'especies_marinas_cites_ii', 
        ('especies Marinas III'):'especies_marinas_cites_iii', 
        ('registros Marinos appendixCITES'):'registros_marinas_cites_total', 
        ('registros Marinos I'):'registros_marinas_cites_i', 
        ('registros Marinos I/II'):'registros_marinas_cites_i_ii',
        ('registros Marinos II'):'registros_marinas_cites_ii', 
        ('registros Marinos III'):'registros_marinas_cites_iii', 
        'especies_marinas_exoticas_total':'especies_marinas_exoticas_total', 
        ('especies Marinas Exótica'):'especies_marinas_exoticas', 
        ('especies Marinas Invasora'):'especies_marinas_invasoras',
        'especies_marinas_exoticas_riesgo_invasion_total':'especies_marinas_exoticas_riesgo_invasion_total',        
        ('especies Marinas Exótica con potencial de invasión Alto Riesgo'):'especies_marinas_exoticas_riesgo_invasion_alto',
        ('especies Marinas Exótica con potencial de invasión Bajo Riesgo'):'especies_marinas_exoticas_riesgo_invasion_bajo', 
        ('especies Marinas Exótica con potencial de invasión Riesgo Moderado'):'especies_marinas_exoticas_riesgo_invasion_moderado',  
        ('especies Marinas Exótica con potencial de invasión Riesgo Moderado/ Alto'):'especies_marinas_exoticas_riesgo_invasion_moderado_alto',          
        ('especies Marinas Trasplantada'):'especies_marinas_trasplantadas',
        'registros_marinas_exoticas_total':'registros_marinas_exoticas_total',
        ('registros Marinos Exótica'):'registros_marinas_exoticas', 
        ('registros Marinos Invasora'):'registros_marinas_invasoras',
        'registros_marinas_exoticas_riesgo_invasion_total':'registros_marinas_exoticas_riesgo_invasion_total',
        ('registros Marinos Exótica con potencial de invasión Alto Riesgo'):'registros_marinas_exoticas_riesgo_invasion_alto',        
        ('registros Marinos Exótica con potencial de invasión Bajo Riesgo'):'registros_marinas_exoticas_riesgo_invasion_bajo',        
        ('registros Marinos Exótica con potencial de invasión Riesgo Moderado'):'registros_marinas_exoticas_riesgo_invasion_moderado',        
        ('registros Marinos Exótica con potencial de invasión Riesgo Moderado/ Alto'):'registros_marinas_exoticas_riesgo_invasion_moderado_alto',        
        ('registros Marinos Trasplantada'):'registros_marinas_trasplantadas',
        ('especies Marinas endemic'):'especies_marinas_endemicas', 
        ('especies Marinas migratory'):'especies_marinas_migratorias', 
        ('registros Marinos endemic'):'registros_marinas_endemicas',     
        ('registros Marinos migratory'):'registros_marinas_migratorias', 
        'especies_marinas_amenazadas_global_total':'especies_marinas_amenazadas_global_total',
        #('especies', 'threatStatus_UICN'):'especies_marinas_amenazadas_global_total',  
        ('especies Marinas EX_IUCN'):'especies_marinas_amenazadas_global_ex', 
        ('especies Marinas EW_IUCN'):'especies_marinas_amenazadas_global_ew', 
        ('especies Marinas CR_IUCN'):'especies_marinas_amenazadas_global_cr', 
        ('especies Marinas EN_IUCN'):'especies_marinas_amenazadas_global_en', 
        ('especies Marinas VU_IUCN'):'especies_marinas_amenazadas_global_vu',
        ('especies Marinas NT_IUCN'):'especies_marinas_amenazadas_global_nt', 
        ('especies Marinas LC_IUCN'):'especies_marinas_amenazadas_global_lc', 
        ('especies Marinas DD_IUCN'):'especies_marinas_amenazadas_global_dd',
        ('especies Marinas LR/lc_IUCN'):'especies_marinas_amenazadas_global_lr_lc', 
        ('especies Marinas LR/nt_IUCN'):'especies_marinas_amenazadas_global_lr_nt',
        'registros_marinas_amenazadas_global_total':'registros_marinas_amenazadas_global_total',
        ('registros Marinos EX_IUCN'):'registros_marinas_amenazadas_global_ex', 
        ('registros Marinos EW_IUCN'):'registros_marinas_amenazadas_global_ew', 
        ('registros Marinos CR_IUCN'):'registros_marinas_amenazadas_global_cr', 
        ('registros Marinos EN_IUCN'):'registros_marinas_amenazadas_global_en', 
        ('registros Marinos VU_IUCN'):'registros_marinas_amenazadas_global_vu', 
        ('registros Marinos NT_IUCN'):'registros_marinas_amenazadas_global_nt', 
        ('registros Marinos LC_IUCN'):'registros_marinas_amenazadas_global_lc',
        ('registros Marinos DD_IUCN'):'registros_marinas_amenazadas_global_dd',    
        ('registros Marinos LR/lc_IUCN'):'registros_marinas_amenazadas_global_lr_lc', 
        ('registros Marinos LR/nt_IUCN'):'registros_marinas_amenazadas_global_lr_nt',
        
        ('especies Salobres threatStatus_MADS'):'especies_salobres_amenazadas_nacional_total', 
        ('especies Salobres CR_MADS'):'especies_salobres_amenazadas_nacional_cr',
        ('especies Salobres EN_MADS'):'especies_salobres_amenazadas_nacional_en', 
        ('especies Salobres VU_MADS'):'especies_salobres_amenazadas_nacional_vu',
        ('registros Salobres threatStatus_MADS'):'registros_salobres_amenazadas_nacional_total', 
        ('registros Salobres CR_MADS'):'registros_salobres_amenazadas_nacional_cr', 
        ('registros Salobres EN_MADS'):'registros_salobres_amenazadas_nacional_en',
        ('registros Salobres VU_MADS'):'registros_salobres_amenazadas_nacional_vu',  
        ('especies Salobres appendixCITES'):'especies_salobres_cites_total', 
        ('especies Salobres I'):'especies_salobres_cites_i', 
        ('especies Salobres I/II'):'especies_salobres_cites_i_ii',
        ('especies Salobres II'):'especies_salobres_cites_ii', 
        ('especies Salobres III'):'especies_salobres_cites_iii', 
        ('registros Salobres appendixCITES'):'registros_salobres_cites_total', 
        ('registros Salobres I'):'registros_salobres_cites_i', 
        ('registros Salobres I/II'):'registros_salobres_cites_i_ii',
        ('registros Salobres II'):'registros_salobres_cites_ii', 
        ('registros Salobres III'):'registros_salobres_cites_iii', 
        'especies_salobres_exoticas_total':'especies_salobres_exoticas_total', 
        ('especies Salobres Exótica'):'especies_salobres_exoticas', 
        ('especies Salobres Invasora'):'especies_salobres_invasoras',
        'especies_salobres_exoticas_riesgo_invasion_total':'especies_salobres_exoticas_riesgo_invasion_total',        
        ('especies Salobres Exótica con potencial de invasión Alto Riesgo'):'especies_salobres_exoticas_riesgo_invasion_alto',
        ('especies Salobres Exótica con potencial de invasión Bajo Riesgo'):'especies_salobres_exoticas_riesgo_invasion_bajo', 
        ('especies Salobres Exótica con potencial de invasión Riesgo Moderado'):'especies_salobres_exoticas_riesgo_invasion_moderado',  
        ('especies Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto'):'especies_salobres_exoticas_riesgo_invasion_moderado_alto',          
        ('especies Salobres Trasplantada'):'especies_salobres_trasplantadas',
        'registros_salobres_exoticas_total':'registros_salobres_exoticas_total',
        ('registros Salobres Exótica'):'registros_salobres_exoticas', 
        ('registros Salobres Invasora'):'registros_salobres_invasoras',
        'registros_salobres_exoticas_riesgo_invasion_total':'registros_salobres_exoticas_riesgo_invasion_total',
        ('registros Salobres Exótica con potencial de invasión Alto Riesgo'):'registros_salobres_exoticas_riesgo_invasion_alto',        
        ('registros Salobres Exótica con potencial de invasión Bajo Riesgo'):'registros_salobres_exoticas_riesgo_invasion_bajo',        
        ('registros Salobres Exótica con potencial de invasión Riesgo Moderado'):'registros_salobres_exoticas_riesgo_invasion_moderado',        
        ('registros Salobres Exótica con potencial de invasión Riesgo Moderado/ Alto'):'registros_salobres_exoticas_riesgo_invasion_moderado_alto',        
        ('registros Salobres Trasplantada'):'registros_salobres_trasplantadas',
        ('especies Salobres endemic'):'especies_salobres_endemicas', 
        ('especies Salobres migratory'):'especies_salobres_migratorias', 
        ('registros Salobres endemic'):'registros_salobres_endemicas',     
        ('registros Salobres migratory'):'registros_salobres_migratorias', 
        'especies_salobres_amenazadas_global_total':'especies_salobres_amenazadas_global_total',
        #('especies', 'threatStatus_UICN'):'especies_salobres_amenazadas_global_total',  
        ('especies Salobres EX_IUCN'):'especies_salobres_amenazadas_global_ex', 
        ('especies Salobres EW_IUCN'):'especies_salobres_amenazadas_global_ew', 
        ('especies Salobres CR_IUCN'):'especies_salobres_amenazadas_global_cr', 
        ('especies Salobres EN_IUCN'):'especies_salobres_amenazadas_global_en', 
        ('especies Salobres VU_IUCN'):'especies_salobres_amenazadas_global_vu',
        ('especies Salobres NT_IUCN'):'especies_salobres_amenazadas_global_nt', 
        ('especies Salobres LC_IUCN'):'especies_salobres_amenazadas_global_lc', 
        ('especies Salobres DD_IUCN'):'especies_salobres_amenazadas_global_dd',
        ('especies Salobres LR/lc_IUCN'):'especies_salobres_amenazadas_global_lr_lc', 
        ('especies Salobres LR/nt_IUCN'):'especies_salobres_amenazadas_global_lr_nt',
        'registros_salobres_amenazadas_global_total':'registros_salobres_amenazadas_global_total',
        ('registros Salobres EX_IUCN'):'registros_salobres_amenazadas_global_ex', 
        ('registros Salobres EW_IUCN'):'registros_salobres_amenazadas_global_ew', 
        ('registros Salobres CR_IUCN'):'registros_salobres_amenazadas_global_cr', 
        ('registros Salobres EN_IUCN'):'registros_salobres_amenazadas_global_en', 
        ('registros Salobres VU_IUCN'):'registros_salobres_amenazadas_global_vu', 
        ('registros Salobres NT_IUCN'):'registros_salobres_amenazadas_global_nt', 
        ('registros Salobres LC_IUCN'):'registros_salobres_amenazadas_global_lc',
        ('registros Salobres DD_IUCN'):'registros_salobres_amenazadas_global_dd',    
        ('registros Salobres LR/lc_IUCN'):'registros_salobres_amenazadas_global_lr_lc', 
        ('registros Salobres LR/nt_IUCN'):'registros_salobres_amenazadas_global_lr_nt',
    
        }


        # 1. RENOMBRAR DIRECTO
        archivo = archivo.rename(columns=names)
        
        # 2. ELIMINAR DUPLICADAS
        archivo = archivo.loc[:, ~archivo.columns.duplicated()]
        

        # 3. ORDENAR SEGÚN DICCIONARIO (CLAVE)
        orden_deseado = [names[col] for col in names if names[col] in archivo.columns]
        
        archivo = archivo[orden_deseado]

        ##Quitar .0 al final de las cifras
        archivo=archivo.replace(np.nan,'',regex=True)
        archivo=archivo.astype(str)
        
        archivo=archivo.replace(to_replace='\.0+$',value="",regex=True)

        
        #Cifras finales por grupo biológico y temática
        ##Para grupos biologicos
        if tipo== 1:
            archivo.to_csv(nombre+'region_grupo.tsv',sep='\t', index=False )
            archivo.to_excel(nombre+'region_grupo.xlsx', sheet_name='cifrasGruposBiologicos', index=False )
    
        ##Para cifras geograficas
        if tipo==2:
            archivo.to_csv(nombre+'region_tematica.tsv',sep='\t', index=False )
            archivo.to_excel(nombre+'region_tematica.xlsx', sheet_name='cifrasGeográficas', index=False )
            
    ##-------------------------------------------------16. Ejecución de la función ajuste_nombres ------------------------------------------------##
    ##Se llama la función ingresando los dos parametros requeridos
    #Para grupos biologicos
    
    ajuste_nombres(grupos_biologicos_total, 1)
    
    #Para cifras geográficas
    ajuste_nombres(geografia_total, 2)

    #del registros 
    del grupos_biologicos_total
    del geografia_total
   
##-------------------------------------------------17. Ejecución de la función ejecucion_cifras ------------------------------------------------##
    
'''
La variable 'tipo' permite condicionar los procesos teniendo en cuenta si se van a sacar cifras departamentales o municipales y si 
el conjunto de datos contiene información de registros marítimos
Colombia con datos marinos='CCDM'
Colombia sin datos marinos='CSDM'
Departamental con datos marinos='DCDM'
Departamental sin datos marinos='DSDM'
Municipal con datos marinos='MCDM'
Municipal sin datos marinos='MSDM'

Al seleccionar alguna de las opciones sin datos marinos, las cifras se calculan en forma general, sin discriminar por los hábitat marino, 
terrestre y salobre

Para las opciones con datos marinos, se realiza el cálculo de cifras general y adicionalmente el cálculo para los hábitat marino, terrestre 
y salobre. Es importante aclarar que debido a los hábitos y distribución de las especies se pueden encontrar especies presentes en más de 
un hábitat por lo tanto, el valor general no corresponde a la suma de los valores para cada hábitat.
#ejecucion_cifras (registros,'DSDM','Nombre')  
#ejecucion_cifras (registros,'DSDM','terittorial')  
#ejecucion_cifras (registros,'DSDM','Categoría')  
'''

#ejecucion_cifras (registros,'CSDM','slug_col')  
ejecucion_cifras (registros,'CCDM','slug_col')  
#ejecucion_cifras (registros,'DCDM','slug_x') 
#ejecucion_cifras (registros,'DSDM','slug_x') 
#ejecucion_cifras (registros,'MCDM','slug_y')
#ejecucion_cifras (registros,'MSDM','slug_y')
fin=time.time()    
print((fin-inicio)/60)

registros.to_csv('prueba5.tsv',sep='\t', index=False )