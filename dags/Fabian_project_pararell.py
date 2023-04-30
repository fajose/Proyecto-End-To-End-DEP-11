from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from utils.mysql_utils import MysqlConnection
from utils.mongodb_utils import MongoDBConnection
from utils.gcp_utils import CloudStorageConnector

import os
import yaml
from io import StringIO

import pandas as pd

path = os.path.dirname(__file__)

with open(os.path.join(path, '/user/app/ProyectoEndToEndPython/Proyecto/config/config.yaml')) as file:
    config = yaml.safe_load(file)
    
ip, port, dbname = config['ip'], config['port'], config['dbname']
uri = config['uri_mongo']

bucket = config['google_bucket']
auth_path = config['google_auth_path']

landing_prefix=f'landing2/{dbname}'
gold_prefix = f'gold2/{dbname}'

storage = CloudStorageConnector(bucket, auth_path)

@task
def read_mysql_table(table):
    mysql_connector = MysqlConnection(ip, port, dbname)
    df= mysql_connector.read_mysql(table)   
    
    storage.load_to_storage(df, prefix=landing_prefix, name=table)
    mysql_connector.close_mysql_connection()

def get_mysql_tables():
    mysql_connector = MysqlConnection(ip, port, dbname)
    tables = mysql_connector.get_all_mysql_tables()
    
    return tables    

@task
def read_mongodb_collection(collection): 
    mongodb_connector = MongoDBConnection(uri, dbname)   
    df = mongodb_connector.read_mongodb_collection(collection)
    
    storage.load_to_storage(df, prefix=landing_prefix, name=collection)
    mongodb_connector.close_mongodb_connection()

def get_mongodb_collections():  
    mongodb_connector = MongoDBConnection(uri, dbname)           
    collections = mongodb_connector.get_all_collections()
    mongodb_connector.close_mongodb_connection()
    return collections     

def ingestion(**kwargs):
    if kwargs['source'] == 'mysql':
        print("Obteniendo Tablas desde MySQL...")
        return get_mysql_tables()
    
    if kwargs['source'] == 'mongodb':
        print("Obteniendo Collecciones desde MongoDB...")
        return get_mongodb_collections()


def transform_retail(**kwargs):
  
    func_dict = {
                1:enunciado1,
                2:enunciado2,
                3:enunciado3,
                4:enunciado4,
                5:enunciado5
            }

    df_enunciado= func_dict[kwargs['enunciado']]()

    print(f"Cargando Transformación {kwargs['enunciado']} a Storage")
    storage.load_to_storage(df_enunciado, gold_prefix, f'enunciado {kwargs["enunciado"]}')
    print(f"Transformación {kwargs['enunciado']} cargada exitosamente")


def enunciado1():
        print("INICIANDO TRANSFORMACIÓN 1...")
        orders = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/orders').download_as_text(encoding="utf-8")))
        order_items = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/order_items').download_as_text(encoding="utf-8")))
        departments = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/departments').download_as_text(encoding="utf-8")))
        categories = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/categories').download_as_text(encoding="utf-8")))
        products = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/products').download_as_text(encoding="utf-8")))

        orders_merged_items = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
        orders_merged_items = orders_merged_items[orders_merged_items.order_status == 'COMPLETE'][["order_id", "order_item_product_id", "order_item_quantity"]]
        
        departmentsxcategories = categories.merge(departments, left_on="category_department_id", right_on="department_id")
        productsxdepartmentsxcategories = departmentsxcategories.merge(products, left_on="category_id", right_on="product_category_id")
        
        productsxdepartmentsxcategories = productsxdepartmentsxcategories[["department_name", "category_name", "product_id"]]
        
        orders_per_department = orders_merged_items.merge(productsxdepartmentsxcategories, left_on="order_item_product_id", right_on="product_id")
        
        orders_per_category_per_department = orders_per_department.groupby(["department_name", "category_name"])["order_item_quantity"].sum().reset_index()
        
        best_category_per_department = orders_per_category_per_department.groupby('department_name', as_index=False).apply(lambda x: x.nlargest(1, 'order_item_quantity'))
        
        print("TRANSFORMACIÓN 1 FINALIZADA!")
        return best_category_per_department[["department_name", "category_name"]]
        

def enunciado2():
    print("INICIANDO TRANSFORMACIÓN 2...")
    orders = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/orders').download_as_text(encoding="utf-8")))
    order_items = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/order_items').download_as_text(encoding="utf-8")))

    full_df = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
    full_df = full_df[
            ["order_date","order_status","order_item_product_id","order_item_subtotal"]
        ]
        
    full_df = full_df[full_df.order_status == 'COMPLETE']
        
        
    total_per_day = full_df.groupby("order_date")["order_item_subtotal"].sum().reset_index()
        
    total_per_day['day_of_week'] = pd.to_datetime(total_per_day.order_date).dt.dayofweek
        
    avg_per_weekday = total_per_day.groupby("day_of_week")["order_item_subtotal"].mean().reset_index()
    print("TRANSFORMACIÓN 2 FINALIZADA!")
    return avg_per_weekday

def enunciado3():
    print("INICIANDO TRANSFORMACIÓN 3...")
    orders = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/orders').download_as_text(encoding="utf-8")))
    customers = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/customers').download_as_text(encoding="utf-8")))

    pending_orders = orders[orders.order_status == 'PENDING']
    pending_ordersxcustomers = pending_orders.merge(customers, left_on="order_customer_id", right_on="customer_id")
    
    pending_ordersxcustomers['full_name'] = pending_ordersxcustomers.customer_fname + ' ' + pending_ordersxcustomers.customer_lname
    
    pending_ordersxcustomers = pending_ordersxcustomers.groupby('full_name')['order_id'].count().reset_index()
    
    print("TRANSFORMACIÓN 3 FINALIZADA!")
    return pending_ordersxcustomers

def enunciado4():
    print("INICIANDO TRANSFORMACIÓN 4...")
    orders = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/orders').download_as_text(encoding="utf-8")))
    order_items = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/order_items').download_as_text(encoding="utf-8")))
    departments = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/departments').download_as_text(encoding="utf-8")))
    categories = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/categories').download_as_text(encoding="utf-8")))
    products = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/products').download_as_text(encoding="utf-8")))
    customers = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/customers').download_as_text(encoding="utf-8")))

    customers['full_name'] = customers.customer_fname + ' ' + customers.customer_lname
    customers = customers[['customer_id', 'full_name']]
    
    orders = orders[orders.order_status == 'COMPLETE']
    
    orders = orders.merge(customers, left_on="order_customer_id", right_on="customer_id")
    orders_merged_items = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
    orders_merged_items = orders_merged_items[["order_id", "full_name", "order_item_product_id", "order_item_quantity"]]
    
    departmentsxcategories = categories.merge(departments, left_on="category_department_id", right_on="department_id")
    productsxdepartmentsxcategories = departmentsxcategories.merge(products, left_on="category_id", right_on="product_category_id")
    
    productsxdepartmentsxcategories = productsxdepartmentsxcategories[["department_name", "category_name", "product_id"]]
    
    orders_per_department = orders_merged_items.merge(productsxdepartmentsxcategories, left_on="order_item_product_id", right_on="product_id")
    
    orders_per_customer_per_department = orders_per_department.groupby(["full_name", "department_name"])["order_item_quantity"].sum().reset_index()
    
    favorite_department_per_customer = orders_per_customer_per_department.groupby("full_name", as_index=False).apply(lambda x: x.nlargest(1, 'order_item_quantity'))
    
    print("TRANSFORMACIÓN 4 FINALIZADA!")
    return favorite_department_per_customer[["full_name", "department_name"]]
    
def enunciado5():
    print("INICIANDO TRANSFORMACIÓN 5...")

    orders = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/orders').download_as_text(encoding="utf-8")))
    order_items = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/order_items').download_as_text(encoding="utf-8")))
    departments = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/departments').download_as_text(encoding="utf-8")))
    categories = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/categories').download_as_text(encoding="utf-8")))
    products = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/products').download_as_text(encoding="utf-8")))
    customers = pd.read_csv(StringIO(storage.bucket.get_blob(f'{landing_prefix}/customers').download_as_text(encoding="utf-8")))

    full_product = products.merge(categories, left_on='product_category_id', right_on='category_id')
    full_product = full_product.merge(departments, left_on='category_department_id', right_on='department_id')
    
    full_df = orders.merge(customers, left_on='order_customer_id', right_on='customer_id')
    full_df = full_df.merge(order_items, left_on='order_id', right_on='order_item_order_id')
    full_df = full_df.merge(full_product, left_on='order_item_product_id', right_on='product_id')
    
    full_df = full_df[[
        'order_id', 'order_date', 'order_status',
            'customer_fname', 'customer_lname', 'customer_street', 'customer_city','customer_state', 'customer_zipcode',
            'department_name', 'category_name', 'product_name',
            'order_item_quantity','order_item_product_price', 'order_item_subtotal'
    ]]
    
    print("TRANSFORMACIÓN 5 FINALIZADA!")
    return full_df

args = {"owner": "Fabian", "start_date":days_ago(1)}

@dag(
    dag_id="Fabian_project_pararell",
    default_args=args,
    schedule_interval='@once',
)
def etl():    
    with TaskGroup(group_id="get_data") as getdata:
        get_from_mysql = PythonOperator(task_id="mysql_tables", python_callable=ingestion, op_kwargs={"source": "mysql"}, provide_context=True,)
        get_from_mongodb = PythonOperator(task_id="mongodb_collections", python_callable=ingestion, op_kwargs={"source": "mongodb"}, provide_context=True,)

    with TaskGroup(group_id="ingest") as ingest:
        ingest_from_mysql = read_mysql_table.expand(table=get_from_mysql.output)
        ingest_from_mongo_db = read_mongodb_collection.expand(collection=get_from_mongodb.output)

    with TaskGroup(group_id="Transform") as transform:
        run_transform_enunciado1 = PythonOperator(task_id="transform_enunciado1", python_callable=transform_retail, op_kwargs={"enunciado": 1}, provide_context=True,)
        run_transform_enunciado2 = PythonOperator(task_id="transform_enunciado2", python_callable=transform_retail, op_kwargs={"enunciado": 2}, provide_context=True,)
        run_transform_enunciado3 = PythonOperator(task_id="transform_enunciado3", python_callable=transform_retail, op_kwargs={"enunciado": 3}, provide_context=True,)
        run_transform_enunciado4 = PythonOperator(task_id="transform_enunciado4", python_callable=transform_retail, op_kwargs={"enunciado": 4}, provide_context=True,)
        run_transform_enunciado5 = PythonOperator(task_id="transform_enunciado5", python_callable=transform_retail, op_kwargs={"enunciado": 5}, provide_context=True,)
    
    getdata >> ingest >> transform

print("Iniciando ETL...")
etl()
print("ETL completada con éxito!")





