# Realiza la descarga de los archivos y su posterior pre-procesamiento
import requests
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
import warnings
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account

if not os.path.isdir("data"):
    os.mkdir("data")
    print("Se creo el folder data")

#Se guarda el url de todos los archivos
url = 'https://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/bases-de-datos'
reqs = requests.get(url)
soup = BeautifulSoup(reqs.text, 'html.parser')

url_dict={}
for link in soup.find_all('a'):
    if  link.get('href') != None:
        if "Destino" in link.get('href'):
            url_dict[link.get('href').split("Destino")[-1].strip()] =link.get('href')

#Se descargan los archivos y se almacenan
print("Descargando archivos: ")
for key in tqdm(url_dict.keys()):
    response= requests.get(url_dict[key])
    path = os.path.join(os.getcwd(), "data", key)
    open(path, "wb").write(response.content)


#Se agregan el path a todos los nombres de archivo
files = [os.path.join(os.getcwd(), "data", x) for x in os.listdir(os.path.join(os.getcwd(),"data"))]


#Se guarda la direccion de los archivos de interes y se eliminan de la lista original 
esp1 = [x for x in files if ("Noviembre 2019" in x)  ]
files.pop(files.index(esp1[0]))
esp2  = [x for x in files if ("Julio 2021" in x)   ] 
files.pop(files.index(esp2[0]))

#Se almacenan los archivos de interés en un diccionario
# Estos archivos deben leerse a parte ya que tienen varias hojas 
data_dict = {}
data_dict["Noviembre 2019"]= pd.concat([pd.read_excel(esp1[0], "Página1_1", header=1), pd.read_excel(esp1[0], "Página1_2", header=1), pd.read_excel(esp1[0], "Página1_3", header=1) ], ignore_index=True)
data_dict["Julio 2021"] = pd.read_excel(esp2[0], "DATOS", header=1)


#Se agregan los archivos restantes al diccionario

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

print("Leyendo archivos: ")
for file in  tqdm(files):
    data = pd.read_excel(file)
    data_dict[file.split("\\")[-1]] = data

warnings.filterwarnings("default", category=UserWarning)
data_dict2 = data_dict 

#Se recorre cada dataframe y se eliminan las filas de header
for key in tqdm(data_dict.keys()):
    for index, row in data_dict[key].iterrows():
        if any([x == "Fecha" for x in row]) | any([x == "Destino" for x in row]) :
            data_dict[key].columns = data_dict[key].iloc[index,:].str.lower()
            data_dict[key] = data_dict[key].iloc[index+1:,:].reset_index(drop=True)

            break

#Se pone el nombre de las columnas en minuscula              
for key in tqdm(data_dict.keys()):
    data_dict[key].columns = [x.lower() for x in data_dict[key].columns ]




#Se modfica el nombre de las columnas 
dict_columnas = {'sigla empresa': "sigla", 'sigla iata' : "sigla", "Sigla Empresa":"sigla",
                   'ciudad origen':'ciudad origen','Ciudad Origen':'ciudad origen',
                   "número de mes":"mes",
                   "pais origen":"pais origen",
                     'tráfico (n/i)':"trafico", 'tráfico':"trafico",
                     'tipo vuelo agrupado': "tipo vuelo", "tipovuelo": 'tipo vuelo',
                     "fecha visual":"fecha"}

print("renombrar columnas: ")
for key in tqdm(data_dict.keys()):
  data_dict[key].rename(columns=dict_columnas, inplace=True)  
  
  ind = data_dict[key].iloc[:,:].isnull().sum(axis=1) < 10
  data_dict[key] = data_dict[key].loc[ind,:]
 

#Se definen las columnas que se desean exportar 
cols = ["origen", "destino", "pasajeros", "trafico", "tipo vuelo",
 "ciudad origen", "ciudad destino", "pais origen", "pais destino", "fecha" ]



data_dict["Mes 1992 - 1993.xlsx"]["fecha"]= data_dict["Mes 1992 - 1993.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1992 - 1993.xlsx"]["mes"].astype("str") 
data_dict["Mes 1994.xlsx"]["fecha"]= data_dict["Mes 1994.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1994.xlsx"]["mes"].astype("str") 
data_dict["Mes 1995.xlsx"]["fecha"]= data_dict["Mes 1995.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1995.xlsx"]["mes"].astype("str") 
data_dict["Mes 1996.xlsx"]["fecha"]= data_dict["Mes 1996.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1996.xlsx"]["mes"].astype("str") 
data_dict["Mes 1997.xlsx"]["fecha"]= data_dict["Mes 1997.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1997.xlsx"]["mes"].astype("str") 
data_dict["Mes 1998.xlsx"]["fecha"]= data_dict["Mes 1998.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1998.xlsx"]["mes"].astype("str") 
data_dict["Mes 1999.xlsx"]["fecha"]= data_dict["Mes 1999.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 1999.xlsx"]["mes"].astype("str") 
data_dict["Mes 2000.xlsx"]["fecha"]= data_dict["Mes 2000.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2000.xlsx"]["mes"].astype("str") 
data_dict["Mes 2001.xlsx"]["fecha"]= data_dict["Mes 2001.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2001.xlsx"]["mes"].astype("str") 
data_dict["Mes 2002.xlsx"]["fecha"]= data_dict["Mes 2002.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2002.xlsx"]["mes"].astype("str") 
data_dict["Mes 2003.xlsx"]["fecha"]= data_dict["Mes 2003.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2003.xlsx"]["mes"].astype("str") 
data_dict["Mes 2004.xlsx"]["fecha"]= data_dict["Mes 2004.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2004.xlsx"]["mes"].astype("str") 
data_dict["Mes 2005.xlsx"]["fecha"]= data_dict["Mes 2005.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2005.xlsx"]["mes"].astype("str") 
data_dict["Mes 2006.xlsx"]["fecha"]= data_dict["Mes 2006.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2006.xlsx"]["mes"].astype("str") 
data_dict["Mes 2007.xlsx"]["fecha"]= data_dict["Mes 2007.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2007.xlsx"]["mes"].astype("str")
data_dict["Mes 2008.xlsx"]["fecha"]= data_dict["Mes 2008.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2008.xlsx"]["mes"].astype("str") 
data_dict["Mes 2009.xlsx"]["fecha"]= data_dict["Mes 2009.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2009.xlsx"]["mes"].astype("str") 
data_dict["Mes 2010.xlsx"]["fecha"]= data_dict["Mes 2010.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2010.xlsx"]["mes"].astype("str") 
data_dict["Mes 2011.xlsx"]["fecha"]= data_dict["Mes 2011.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2011.xlsx"]["mes"].astype("str")
data_dict["Mes 2012.xlsx"]["fecha"]= data_dict["Mes 2012.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2012.xlsx"]["mes"].astype("str") 
data_dict["Mes 2013.xlsx"]["fecha"]= data_dict["Mes 2013.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2013.xlsx"]["mes"].astype("str") 
data_dict["Mes 2014.xlsx"]["fecha"]= data_dict["Mes 2014.xlsx"]["mes"].astype("str") + "-" +  data_dict["Mes 2014.xlsx"]["año"].astype("str") 
data_dict["Mes 2015.xlsx"]["fecha"]= data_dict["Mes 2015.xlsx"]["año"].astype("str") + "-" +  data_dict["Mes 2015.xlsx"]["mes"].astype("str")


#Se concatenan las diferentes tablas
df = pd.DataFrame()
for key in tqdm(data_dict.keys()):
  df = pd.concat([df,data_dict[key].loc[:,cols]])

print("Se han concatenado las tablas")


df.loc[df.fecha.astype("str").str.len() < 19,"fecha"] = (df.loc[df.fecha.astype("str").str.len() < 19,"fecha"].str.replace("Jul","07").str.
                replace("May","05").str.replace("Jun","06").str.replace("Ago","08").
                str.replace("Ene","01").str.replace("Feb","02").str.replace("Mar","03").
                str.replace("Abr","04").str.replace("Sep","09").str.replace("Oct","10").
                str.replace("Nov","11").str.replace("Dic","12"))
df.loc[df.fecha.astype("str").str.len() < 19,"fecha"] = df.loc[df.fecha.astype("str").str.len() < 19,"fecha"] + "-01"


df["fecha"] = pd.to_datetime(df.fecha, yearfirst=True)
df["pasajeros"] = df["pasajeros"].astype("Int64")
df["destino"] = df["destino"].astype("str")
df["trafico"] = df["trafico"].astype("str")
df["tipo vuelo"] = df["tipo vuelo"].astype("str")

df.columns = [x.replace(" ","_") for x in df.columns]





#Se carga el archivo a BQ
cred_path = 'key_cred.json'
credentials = service_account.Credentials.from_service_account_file(cred_path)

project_id = 'constant-tracer-334800'
client = bigquery.Client(credentials= credentials,project=project_id)


job_config = bigquery.LoadJobConfig()
table_id = 'constant-tracer-334800.trafico_aereo_col.air_traffic'
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
#time_partitioning = bigquery.table.TimePartitioning(type_=bigquery.TimePartitioningType.YEAR,field="fecha")
#job_config.time_partitioning =  time_partitioning
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
job.result()  # Wait for the job to complete.



table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


for file in os.listdir(os.path.join(os.getcwd(), "data")):
    os.remove(os.path.join(os.getcwd(), "data", file))

os.rmdir('data') 
#Se exporta el dataset como csv
df.to_csv("trafico_col.csv")