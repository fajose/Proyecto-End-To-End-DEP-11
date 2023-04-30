from utils.gcp_utils import CloudStorageConnector, BigQueryConnector
import pandas as pd

class Load:
    def __init__(self):
        self.process = 'Proceso de Carga'
        self.google_storage = None
        self.azure_storage = None
        
    def set_google_storage(self, bucket, auth_path):  
        self.bucket = bucket
        
        self.google_storage = CloudStorageConnector(self.bucket, auth_path) 
        self.bigquery = BigQueryConnector(auth_path) 
        
    def set_azure_storage(self, conn_str, container):
        self.container = container
        
        self.azure_storage = AzureStorageConnector(container, conn_str)
        
    def load_to_azure_storage(self, df_dict, prefix=''):
        if self.azure_storage is not None:
            for name, df in df_dict.items():
                self.azure_storage.load_to_storage(df, prefix=prefix, name=name)
        else:
            raise Exception('Configure conexion a storage primero')        
    
    def load_to_google_storage(self, df_dict, prefix=''):
        if self.google_storage is not None:
            for name, df in df_dict.items():
                self.google_storage.load_to_storage(df, prefix=prefix, name=name)
        else:
            raise Exception('Configure conexion a storage primero')
            
    def load_to_bigquery_from_storage(self, dataset, prefix=''):
        if self.google_storage is not None:
            blobs = self.google_storage.get_all_blobs(prefix)
            
            for blob in blobs:
                name = blob.name.split('/')[-1]
                
                uri = f'gs://{self.bucket}/{prefix}{name}'                
                table_id = f'{dataset}.{name}'                
                self.bigquery.load_from_storage(uri, table_id)
                
        else:
            raise Exception('Configure conexion a storage primero')       
            
    def load_to_bigquery_from_df(self, df_dict, dataset_id):
        for name, df in df_dict.items():
            table_id = f'{dataset_id}.{name}'
            
            self.bigquery.load_from_dataframe(df, table_id)
        