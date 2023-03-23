import requests
import os
from bs4 import BeautifulSoup
from tqdm import tqdm
import polars as pl
from tqdm import tqdm
import os

def download_files():
    """
    Descarga lor archivos del tráfico aéreo alojados en la página de la aerocivil y los
    almacena en la carpeta 'raw'
    """
    #Se comprueba que no exista el folder 'raw'
    if not os.path.isdir("raw"):
        os.mkdir("raw")
        print("Se creo el folder raw")
    else:
        print("El folder 'raw' ya existe en este directorio")

    if len(os.listdir("raw"))== 0:
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
            path = os.path.join(os.getcwd(), "raw", key)
            open(path, "wb").write(response.content)
    else:
        print("Ya se han descargado los archivos ({} en el directorio 'raw')".format(len(os.listdir("raw"))))

    #Se agregan al path a todos los nombres de archivo
    files =[os.path.join("raw",x) for x in os.listdir("raw")]
    return files


#Se extrae el esquema de cada archivo
def get_schema(file):
   """
   Lee todos los archivos descargados y los abre, regresando su esquema
   """
   #Listado de archivos que deben leerse con 'skiprows' 1,3,4 y 5
   sr3 = [os.path.join("raw",nf ) for nf in ['Abril 2019.xlsx','Mes 2000.xlsx','Mes 1998.xlsx','Mes 1997.xlsx','Mes 1995.xlsx','Mayo 2019.xlsx',
                                              'Enero 2022.xlsx','Marzo 2019 V 2.0.xlsx','Febrero 2019 Version 2.0.xlsx']]
   sr4 = [os.path.join("raw",nf ) for nf in ['Enero - Diciembre 2017.xlsx','Ene-Dic 2018.xlsx','Mes 2012.xlsx','Mes 2010.xlsx','Mes 2009.xlsx',
                                              'Mes 2003.xlsx','Mes 2007.xlsx','Mes 2006.xlsx','Mes 2005.xlsx','Mes 2004.xlsx','Mes 2002.xlsx','Mes 2001.xlsx',
                                              'Mes 1999.xlsx','Mes 1996.xlsx','Mes 1994.xlsx',"Mes 1992 - 1993.xlsx",'Mes 2016.xlsx','Septiembre 2022.xlsx','Octubre 2022.xlsx',
                                              'Noviembre 2022.xlsx','Mes 2014.xlsx','Mayo 2022.xlsx','Junio 2022.xlsx','Julio 2022.xlsx','Enero 2019.xlsx', "Junio 2019.xlsx",
                                              "Agosto 2022.xlsx","Agosto 2022", "Diciembre 2022.xlsx", 'Enero - Diciembre 2017.xlsx', 'Enero 2019.xlsx']]
   sr1 = [os.path.join("raw",nf ) for nf in ["Enero 2021.xlsx"]]
   sr5 = [os.path.join("raw",nf ) for nf in ["Mes 2015.xlsx", 'Mes 2008.xlsx','Mes 2011.xlsx', 'Mes 2013.xlsx']]

   #Casos especiales
   if "Julio 2021.xlsx" in file:
      df = pl.read_excel(os.path.join("raw",'Julio 2021.xlsx'), sheet_name="DATOS", read_csv_options= {"infer_schema_length":50000})
   elif file in sr3:
      df= pl.read_excel(file,read_csv_options= {"infer_schema_length":500000,"skip_rows":3} )
   elif file in sr4:
      df= pl.read_excel(file,read_csv_options= {"infer_schema_length":500000,"skip_rows":4} )
   elif file in sr5:
      df= pl.read_excel(file,read_csv_options= {"infer_schema_length":5000000,"skip_rows":5} )
   elif file in sr1:
      df = pl.read_excel(file, read_csv_options= {"infer_schema_length":500000,"skip_rows":1})    
   else:
      df=pl.read_excel(file, read_csv_options= {"infer_schema_length":500000})
   schema = {"filename":file, "columns":df.columns, "types":df.dtypes}

   return schema,df

schemas =[]
