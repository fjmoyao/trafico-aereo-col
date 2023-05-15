# Predicción de Series de Tiempo para el Tráfico Aéreo en Colombia

Este proyecto tiene como objetivo construir un modelo de predicción de series de tiempo para predecir el tráfico aéreo en diferentes aerolíneas con actividad en Colombia.

## Instalación
Siga los pasos a continuación para instalar y configurar el proyecto en su máquina local:

1. Clone el repositorio desde GitHub:
   ```
   git clone https://github.com/fjmoyao/trafico-aereo-col.git
   ```
2. Instale los requisitos necesarios:
      ```
   pip install -r requirements.txt
   ```
   
## Uso
Para replicar el proceso de predicción, siga los pasos a continuación:

Ejecute los cuadernos en el siguiente orden:

1. Data Wrangling
2. Exploratory Data Analysis (EDA)
3. Model Building 1
4. Model Building 2
Estos cuadernos contienen el código necesario para el procesamiento de datos, análisis exploratorio y construcción de modelos.

Alternativamente, puede utilizar la aplicación para interactuar directamente con los modelos:
   ```
   streamlit run app.py
      ```

La aplicación le permitirá realizar predicciones y explorar los resultados de manera interactiva.

## Uso de Big Data y Bibliotecas

En este proyecto, destacamos el uso de bibliotecas de Big Data como PySpark y Polars, lo que nos permitió manejar y transformar un gran volumen de datos de manera eficiente. Los datos utilizados en este proyecto fueron extraídos de la Aeronáutica Civil de Colombia mediante técnicas de web scraping. Sin embargo, es importante tener en cuenta que el uso de estos datos está sujeto a la ley vigente y las políticas de uso de la institución correspondiente.

## Licencia

Este proyecto se distribuye bajo la licencia MIT. Consulte el archivo LICENSE para obtener más información.


