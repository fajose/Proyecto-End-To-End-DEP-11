import os
from azure.storage.blob import ContainerClient
from utils.storage_utils import StorageConnector

class AzureStorageConnector(StorageConnector):
    def __init__(self, container, conn_str): 
        self.storage = ContainerClient.from_connection_string(conn_str=conn_str, container_name=container)

    def load_to_storage(self, df, prefix, name):
        output, storage_path = super().load_to_storage(df, prefix, name)

        if self.storage is not None:            
            self.container_client.upload_blob(storage_path, output, overwrite=True, encoding='utf-8')
        else:
            raise Exception('No hay conexión a Storage configurada.')
            
    def get_all_blobs(self, prefix=''):
        if self.storage is not None:
            blobs = self.container_client.list_blobs(prefix=prefix)
        else: 
            raise Exception('No hay conexión a Storage configurada.')
        return blobs
    
    def download_blob(self, blob):
        if self.storage is not None:
            downloaded_blob = blob.content_as_text()
        else: 
            raise Exception('No hay conexión a Storage configurada.')
        return downloaded_blob