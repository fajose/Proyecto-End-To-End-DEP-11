import yaml
from process.extract import Extract
from process.load import Load
import os

path = os.path.dirname(__file__)

with open(os.path.join(path, 'config/config.yaml')) as file:
    config = yaml.safe_load(file)
    
ip, port, dbname = config['ip'], config['port'], config['dbname']
uri = config['uri_mongo']

bucket = config['google_bucket']
auth_path = config['google_auth_path']
prefix=f'landing/{dbname}'

print("Iniciando Ingesta de Datos...")

extractor = Extract()
loader = Load()

loader.set_google_storage(bucket=bucket, auth_path=auth_path)

print("Conectando a MySQL...")
df_dict_sql = extractor.extract_from_mysql(ip, port, dbname) 

print("Cargando archivos a Storage...")
loader.load_to_google_storage(df_dict_sql, prefix)

print("Conectando a MongoDB...")
df_dict_mongo = extractor.extract_from_mongodb(uri, dbname)

print("Cargando archivos a Storage...")
loader.load_to_google_storage(df_dict_mongo, prefix)

print("Ingesta Finalizada!")