# Realiza la descarga de los archivos y su posterior pre-procesamiento
import requests
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
import warnings
from tqdm import tqdm

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
                     'tipo vuelo agrupado': "tipo vuelo", "tipovuelo": 'tipo vuelo'}

for key in data_dict.keys():
  data_dict[key].rename(columns=dict_columnas, inplace=True)   

#Se definen las columnas que se desean exportar 
cols = [ "destino", "pasajeros", "trafico", "tipo vuelo",
 "ciudad origen", "ciudad destino", "pais origen", "pais destino" ]

#Se concatenan las diferentes tablas
df = pd.DataFrame()
for key in tqdm(data_dict.keys()):
    df = pd.concat([df,data_dict[key].loc[:,cols]])

print("Se han concatenado las tablas")

print(df.shape)

