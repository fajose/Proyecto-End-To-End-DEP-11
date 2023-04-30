import os

class StorageConnector:
    def __init__(self, storage=None, **storage_params):
        self.storage = None

    def load_to_storage(self, df, prefix, name):
        storage_path = f'{prefix}/{name}'

        output = df.to_csv(encoding = "utf-8", index=False)

        return output, storage_path
            
    def get_all_blobs(self, prefix=''):
        return None
    
    def download_blob(self, blob):
        return None
    
            