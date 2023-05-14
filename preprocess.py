#Descarga los archivos y genera el esquema de datos inicial, guardando la informacion
# cruda y con un prepocesamiento inicial en los directorios 'trusted' y 'raw', respectivamente
from helper_functions import download_files, get_schema
import polars as pl
from tqdm import tqdm
import os
#Se descargan los archivos y se almacena su ruta
files = download_files()

#Se consideran los archivos hasta el año 2022
#to_delete = [(x, files.index(x) )for x in files if "2023" in x]
#deleted_items=[]
#for file in to_delete:
#    files.pop(file[1])
#    deleted_items.append(file[0].strip("raw"))
#print("Elementos eliminados {}".format(deleted_items))

def delete_strings_from_list(strings_to_delete, my_list):
    for string in strings_to_delete:
        if string in my_list:
            my_list.remove(string)

to_delete = [x for x in files if "2023" in x]
delete_strings_from_list(strings_to_delete=to_delete, my_list=files)
print("Elementos eliminados {}".format(to_delete))

#Se leen los archivos iterativamente y se concatenan en distintos dfs 
# agrupandolos por esquemas similares
if os.path.isfile(os.path.join("trusted","trafico_aereo_1992_2022.csv")):
    print("El archivo 'trafico_aereo_1992_2022.csv' ya existe")
else:
    print("Espere un momento...")
    #Se crean dataframes vacios con los esquemas predeterminados
    files16_cols =['Fecha','Sigla Empresa','Origen','Destino','Pasajeros','Trafico','TipoVuelo','Ciudad Origen','Ciudad Destino','Pais Origen','Pais Destino','Nombre Empresa','Apto_Origen','Apto_Destino']
    df16 = pl.read_excel(os.path.join("raw",'Marzo 2019 V 2.0.xlsx'), read_csv_options= {"skip_rows":3}).select(files16_cols).clear()

    files17_cols =['Sigla Empresa','Nombre','Fecha','Año','Número de Mes','Origen','Nombre_duplicated_0','Ciudad Origen','Pais Origen','Destino','Nombre_duplicated_1','Ciudad Destino','Pais Destino','Tráfico (N/I)','Tipo Vuelo','Pasajeros']
    df17 = pl.read_excel(os.path.join("raw",'2020 Noviembre.xlsx')).select(files17_cols).clear()

    files18_cols =  ['Sigla Empresa','Nombre','Fecha','Año','Número de Mes','Origen','Apto_Origen','Ciudad Origen','Pais Origen','Destino','Apto_Destino','Ciudad Destino','Pais Destino','Tráfico (N/I)','Tipo Vuelo','Pasajeros']
    df18 = pl.read_excel(os.path.join("raw",'Agosto 2019.xlsx'), read_csv_options= {"skip_rows":0,"infer_schema_length":500000}).select(files18_cols).clear()

    files19_cols = ['Sigla Empresa','Origen','Destino','Pasajeros','Trafico','TipoVuelo','CargaKg','CorreoKg','Ciudad Origen','Ciudad Destino','APTO_ORIGEN','APTO_DESTINO','Pais Origen','Pais Destino','NOMBRE_EMPRESA','AÑO','MES','Continente Origen','Continente Destino']
    df19 =pl.read_excel(os.path.join("raw",'Mes 2013.xlsx'), read_csv_options= {"skip_rows":5,"infer_schema_length":500000}).select(files19_cols).clear()

    files23_cols = ['Fecha','Sigla IATA','Nombre','Origen','Ciudad Origen','Departamento Origen','Pais Origen','Continente Origen','Destino','Sigla IATA_duplicated_0','Nombre_duplicated_0','Ciudad Destino','Departamento Destino','Pais Destino','Continente Destino','Sigla Empresa','Nombre_duplicated_1','Actividad','Pasajeros','Carga (Kg)','Correo (Kg)','Tráfico (N/I)','Tipo Vuelo Agrupado']
    df23 =pl.read_excel(os.path.join("raw",'Septiembre 2020.xlsx'), read_csv_options= {"skip_rows":0,"infer_schema_length":500000}).select(files23_cols).clear()

    files39_cols= ['Fecha','Sigla Empresa', 'Nombre Empresa', 'Trafico', 'Origen', 'Destino', 'Pasajeros', 'TipoVuelo', 'CargaKg', 'CorreoKg', 'Ciudad Origen','Ciudad Destino','Pais Origen','Pais Destino','Apto_Origen','Apto_Destino']
    df39 =pl.read_excel(os.path.join("raw",'Ene-Dic 2018.xlsx'), read_csv_options= {"skip_rows":4,"infer_schema_length":500000}).select(files39_cols).clear()

    schemas =[]


    #Se leen los archivos iterativamente y se concatenan en distintos dfs 
    # agrupandolos por esquemas similares
    print("Leyendo archivos: ")
    for archivo in tqdm(files):
        schema, df = get_schema(archivo)
        schemas.append(schema)
        tam_cols =df.columns
        #print(archivo)
        #print(len(df.columns))
        if "Mes 2014" in archivo:
            df = df.with_columns(pl.lit("/").alias("_")).with_columns(
                                pl.concat_str(["AÑO","_", "MES"]).alias("Fecha")
                                ).rename({"Tráfico":"Trafico", "Apto Origen":"Apto_Origen",
                                            "Apto Destino":"Apto_Destino"}) 
            df16 = pl.concat([df16, df.select(files16_cols)],how ="vertical")
        
        elif "Mes 2015" in archivo:  
            df = df.rename({"Tráfico (N/I)":"Trafico", "Apto Origen":"Apto_Origen",
                            "Apto Destino":"Apto_Destino", "Tipo Vuelo":"TipoVuelo"}
                            ).with_columns(pl.lit("/").alias("_")).with_columns(
                            pl.concat_str(["Número de Mes","_", "Año"]).alias("Fecha"))
            df16 = pl.concat([df16, df.select(files16_cols)],how ="vertical")

        elif "Diciembre 2019" in archivo:
            df = df.rename({"Nombre_duplicated_0":"Apto_Origen","Nombre_duplicated_1":"Apto_Destino" })
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")

        elif "Mayo 2019" in archivo:
            df = df.rename({"Fecha Visual":"Fecha","Apto Origen":"Apto_Origen", 
                "Apto Destino":"Apto_Destino", "Nombre Empresa":"Nombre",
                "Tráfico":"Tráfico (N/I)"})
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")
        
        elif "Julio 2019" in archivo:
            df = df.rename({"Fecha Visual":"Fecha","Apto Origen":"Apto_Origen", "Apto Destino":"Apto_Destino" })
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")

        elif "Mes 2016" in archivo:
            df = df.rename({ "Nombre Empresa":"Nombre", "Fecha Visual":"Fecha","Tráfico":"Tráfico (N/I)",
                    "Mes":"Número de Mes","Apto Origen":"Apto_Origen", "Apto Destino":"Apto_Destino"})
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")

        elif ("Octubre 2019"  in archivo):
            df = df.rename({ "Nombre_duplicated_0":"Apto_Origen", "Nombre_duplicated_1":"Apto_Destino"})
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")

        elif ("septiembre  2019" in archivo):
            df = df.rename({ "Nombre_duplicated_0":"Apto_Origen", "Nombre_duplicated_1":"Apto_Destino"})
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")
        
        elif len(tam_cols)==16:
            df16 = pl.concat([df16, df.select(files16_cols)],how ="vertical")

        elif len(tam_cols)==17:
            #print(len(df17.columns))
            #print(len(df.select(files17_cols).columns))
            df17 = pl.concat([df17, df.select(files17_cols)],how ="vertical")

        elif len(tam_cols)==18:
            df18 = pl.concat([df18, df.select(files18_cols)],how ="vertical")

        elif len(tam_cols)==19:
            df19 = pl.concat([df19, df.select(files19_cols)],how ="vertical")

        elif len(tam_cols)==23:
            df23 = pl.concat([df23, df.select(files23_cols)],how ="vertical")

        elif len(tam_cols)==39:
            df39 = pl.concat([df39, df.select(files39_cols)],how ="vertical")


#---------------------------------------------------------------------------------------------------------------------------
    #Se definen las columnas a utilizar y se seleccionan solamente estas para cada grupo de dataframes
    cols = ['Fecha','Sigla Empresa','Origen','Destino','Pasajeros','Trafico','TipoVuelo','Ciudad Origen','Ciudad Destino','Pais Origen','Pais Destino','Nombre Empresa','Apto_Origen','Apto_Destino']
                
    df17 = df17.rename({"Nombre_duplicated_0":"Apto_Origen","Nombre_duplicated_1":"Apto_Destino",
                        'Tráfico (N/I)':"Trafico", "Nombre":"Nombre Empresa",
                        'Tipo Vuelo':'TipoVuelo' }).select(cols)

    df18 = df18.rename({'Tráfico (N/I)':"Trafico", "Nombre":"Nombre Empresa",
                        'Tipo Vuelo':'TipoVuelo' }).select(cols)

    df19 = df19.rename({ 'NOMBRE_EMPRESA':"Nombre Empresa",'APTO_ORIGEN':'Apto_Origen',
                        'APTO_DESTINO':'Apto_Destino'}).with_columns(pl.lit("/").alias("_")).with_columns(
                                            pl.concat_str(["MES","_", "AÑO"]).alias("Fecha")).select(cols)

    df23 = df23.rename({"Nombre":"Apto_Origen","Nombre_duplicated_0":"Apto_Destino",
                        'Tráfico (N/I)':"Trafico", "Nombre_duplicated_1":"Nombre Empresa",
                        'Tipo Vuelo Agrupado':'TipoVuelo' }).select(cols)
    df39 = df39.select(cols)
#------------------------------------------------------------------------------------------------------------------------------
    #Concatenacion de toda la informacion
    data_consolidada = pl.concat([df16, df17, df18, df19, df23, df39],how ="vertical").with_columns(
                                    pl.col("Fecha").
                                                str.replace_all("Ene","01").
                                                str.replace_all("Feb","02").
                                                str.replace_all("Mar","03").
                                                str.replace_all("Abr","04").
                                                str.replace_all("May","05").
                                                str.replace_all("Jun","06").
                                                str.replace_all("Jul","07").
                                                str.replace_all("Ago","08").
                                                str.replace_all("Sep","09").
                                                str.replace_all("Oct","10").
                                                str.replace_all("Nov","11").
                                                str.replace_all("Dic","12").
                                                str.replace_all("Jan","01").
                                                str.replace_all("Apr","04").
                                                str.replace_all("Aug","08").
                                                str.replace_all("Dec","12")
                                                )
    
    #Se corrige el formato de fecha
    data_consolidada = data_consolidada.with_columns(
                    pl.col("Fecha").str.lengths().alias("len")).with_columns(
                                pl.
                                when(pl.col("len")==6).
                                then(pl.concat_str([pl.col("Fecha").str.split(by="/").arr.get(1), pl.lit("-"),
                                                    pl.col("Fecha").str.split(by="/").arr.get(0), pl.lit("-"),
                                                    pl.lit("01")])).
                                when((pl.col("len")==8) & pl.col("Fecha").str.contains(r"[0-9]-[0-9]+")).
                                then(pl.concat_str([pl.lit("20"), pl.col("Fecha").str.slice(-2),
                                                    pl.lit("-"),  pl.col("Fecha").str.slice(3,2), 
                                                    pl.lit("-"),  pl.col("Fecha").str.slice(0,2)])).

                                when((pl.col("len")==17)).
                                then(pl.concat_str([pl.lit("20"), pl.col("Fecha").str.slice(6,2),
                                                    pl.lit("-"),  pl.col("Fecha").str.slice(3,2), 
                                                    pl.lit("-"),  pl.col("Fecha").str.slice(0,2)])).
                                when((pl.col("len")==7) & pl.col("Fecha").str.contains(r"[0-9]/[0-9]+")).
                                then(pl.concat_str([pl.col("Fecha").str.slice(-4),
                                                    pl.lit("-"), pl.col("Fecha").str.slice(0,2), 
                                                    pl.lit("-"), pl.lit("01")])).
                                when((pl.col("len")==7) & pl.col("Fecha").str.contains(r"[0-9]-[0-9]+")).
                                then(pl.concat_str([pl.col("Fecha").str.slice(0,4),
                                                    pl.lit("-"), pl.col("Fecha").str.slice(-2), 
                                                    pl.lit("-"), pl.lit("01")])).
                                                
                                otherwise(pl.col("Fecha")).alias("Fecha_corregida")
    )

    #Se convierte la columna de texto a fecha
    data_consolidada = data_consolidada.with_columns(pl.col("Fecha_corregida").str.strptime(pl.Date, fmt="%Y-%m-%d"))
    data_consolidada = data_consolidada.select(
        [pl.col("Fecha_corregida").alias("Fecha"),
         pl.col("Sigla Empresa"),
         pl.col("Origen"),
         pl.col("Destino"),
         pl.col("Pasajeros"),
         pl.col("Trafico"),
         pl.col("TipoVuelo"),
         pl.col("Ciudad Origen"),
         pl.col("Ciudad Destino"),
         pl.col("Pais Origen"),
         pl.col("Pais Destino"),
         pl.col("Nombre Empresa"),
         pl.col("Apto_Origen"),
         pl.col("Apto_Destino")       
        ]
    )
    #Se comprueba que no exista el folder 'trusted'
    if not os.path.isdir("trusted"):
        os.mkdir("trusted")
        print("Se creo el folder trusted")
    else:
        print("El folder 'trusted' ya existe en este directorio")

    data_consolidada.write_csv(os.path.join("trusted","trafico_aereo_1992_2022.csv"))
    print("Se creó el archivo 'trafico_aereo_1992_2022.csv' ")