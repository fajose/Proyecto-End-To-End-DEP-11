Proyecto End to End de Python - Por Fabián Manrique

En la carpeta dags, se encuentras los dags hechos, en versión secuencial y pararela. Copiarlos y pegarlos en la carpeta dag de airflow, junto con los utils, necesarios para el funcionamiento del dag paralelo. Los archivos Ingesta.py y Transformación.py contienen el etl, que abarca los siguientes pasos:

1. Extracción de tablas de una base de datos mysql y una base de datos No relacional de MongoDB.
2. Cargue de archivos extraídos a la capa Landing de un storage de Google.
3. Transformaciones, usando los archivos cargados a la capa Landing.
4. Cargue de tablas transformadas a la capa Gold de un storage de Google.
5. Cargue de tablas transformadas a una base de datos de BigQuery.

En la carpeta process, se encuentran las clases usadas para la Extracción, Carga y Transformación.

En la carpeta utils, se encuentran funciones y clases de apoyo para los distintos procesos, incluyendo la conexión a las distintas fuentes y destinos de datos que se usaron en el proyecto.

En la carpeta config, se debe cargar un archivo .yaml, con las variables de parametrización, estan incluyen:

uri_mongo: Link de conexión a base de datos de MongoDB

dbname: Base de Datos de Origen

ip: Ip para conexión a base de datos local de MySql
port: Puerto para conexión a base de datos local de MySql

google_auth_path: Path a la Key de autenticación de Google
google_bucket: Nombre del Bucket donde se alojarán la capa Landing y la capa Gold.

bigquery: Id de la base de datos de BigQuery.