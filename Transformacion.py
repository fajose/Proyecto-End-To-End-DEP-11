import yaml

from process.extract import Extract
from process.transform import Transform
from process.load import Load

with open('/user/app/ProyectoEndToEndPython/Proyecto/config/config.yaml') as file:
    config = yaml.safe_load(file)
    
ip, port, dbname = config['ip'], config['port'], config['dbname']
uri = config['uri_mongo']

bucket = config['google_bucket']
auth_path = config['google_auth_path']

bigquery = config['bigquery']

print("Iniciando Transformación de Datos...")
extractor = Extract()

print("Extrayendo archivos de la capa Landing...")
df_dict = extractor.extract_from_storage(auth_path, bucket, f'landing/{dbname}')

orders = df_dict['orders']
order_items = df_dict['order_items']
customers = df_dict['customers']
products = df_dict['products']
categories = df_dict['categories']
departments = df_dict['departments']

print("Iniciando transformaciones...")
transformer = Transform()

print("Transformación 1...")
avg_earning_per_weekday = transformer.get_average_earning_per_day_of_week(orders, order_items)

print("Transformación 2...")
best_category_per_department = transformer.get_most_sold_category_per_department(orders, order_items, products, categories, departments)

print("Transformación 3...")
pending_ordersxcustomers = transformer.get_pending_orders_per_customer(orders, customers)

print("Transformación 4...")
favorite_department_per_customer = transformer.get_customer_favorite_department(customers, orders, order_items, products, categories, departments)

print("Transformación 5...")
full_df = transformer.get_full_df(df_dict)
print("Transformaciones completadas con éxito")

print("Iniciando Carga de Archivos Transformados...")
prefix = f'gold/{dbname}'

gold_loader = Load()
gold_loader.set_google_storage(bucket, auth_path)

df_dict = {'avg_earning_per_weekday':avg_earning_per_weekday, 
           'best_category_per_department':best_category_per_department,
           'pending_ordersxcustomers':pending_ordersxcustomers,
           'favorite_department_per_customer':favorite_department_per_customer,
           'full_orders': full_df}

print("Cargando Archivos Transformados a Storage...")
gold_loader.load_to_google_storage(df_dict, prefix)

print("Cargando Archivos Transformados a Bigquery...")
gold_loader.load_to_bigquery_from_df(df_dict, bigquery)

print("Carga Completada!")
print("Proceso Finalizado con Éxito")
