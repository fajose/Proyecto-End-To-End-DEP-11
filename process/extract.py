from utils.mysql_utils import MysqlConnection
from utils.mongodb_utils import MongoDBConnection
from utils.gcp_utils import CloudStorageConnector

from io import StringIO

class Extract:
    def __init__(self):
        self.process = "Proceso de Extracción" 
           
    def extract_from_mysql(self, ip, port, dbname):
        self.mysql_connector = MysqlConnection(ip, port, dbname)
        self.mysql_connector.connect_mysql()
        
        tables = self.mysql_connector.get_all_mysql_tables()
        
        df_dict = {}
        
        for table in tables:
            df_dict[table] = self.mysql_connector.read_mysql(table)                     
        
        self.mysql_connector.close_mysql_connection()
        return df_dict
    
    def extract_from_mongodb(self, uri, dbname):
        self.mongodb_connector = MongoDBConnection(uri, dbname)
        self.mongodb_connector.connect_mongodb()
        
        collections = self.mongodb_connector.get_all_collections()
        
        df_dict = {}
        for collection in collections:            
            df_dict[collection] = self.mongodb_connector.read_mongodb_collection(collection)
            
        self.mongodb_connector.close_mongodb_connection()
        
        return df_dict
    
    def extract_from_storage(self, auth_path, bucket, prefix):
        self.storage = CloudStorageConnector(bucket, auth_path)
        
        blobs = self.storage.get_all_blobs(prefix)
        df_dict = {}
        
        for blob in blobs:
            name = blob.name.split('/')[-1]
            
            df_dict[name] = self.storage.download_blob(blob)
            
        return df_dict
            
        

            
        
        
        
        
        
            
    
        
