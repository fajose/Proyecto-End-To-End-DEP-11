import os
from google.cloud import bigquery
from google.cloud import storage
from utils.storage_utils import StorageConnector

import pandas as pd
from io import StringIO

class CloudStorageConnector(StorageConnector):
    def __init__(self, bucket, auth_path):   
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= auth_path   
        self.storage = storage.Client()
        self.bucket = self.storage.get_bucket(bucket)

    def load_to_storage(self, df, prefix, name):
        output, storage_path = super().load_to_storage(df, prefix, name)
        
        if self.storage is not None:            
            self.bucket.blob(storage_path).upload_from_string(output, 'text/csv')
        else:
            raise Exception('No hay conexión a Storage configurada.')
            
    def get_all_blobs(self, prefix=''):
        if self.storage is not None:
            blobs = self.bucket.list_blobs(prefix=prefix)
        else: 
            raise Exception('No hay conexión a Storage configurada.')
        return blobs
    
    def download_blob(self, blob):
        if self.storage is not None:
            downloaded_blob = blob.download_as_text(encoding="utf-8")
        else: 
            raise Exception('No hay conexión a Storage configurada.')
        df = pd.read_csv(StringIO(downloaded_blob))
        return df
    
class BigQueryConnector:
    def __init__(self, auth_path):   
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= auth_path 
        self.client = bigquery.Client()
        
    def load_from_dataframe(self, df, table_id):
        schema = []
        for column in df.columns:
            if df[column].dtype == 'object':
                schema.append(bigquery.SchemaField(column,bigquery.enums.SqlTypeNames.STRING)) 
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE"
        )

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)

        job.result()

        table = self.client.get_table(table_id)
        
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
            
    def load_from_storage(self, uri, table_id):
        table_schema = self.client.get_table(table_id) # Get schema of destination table

        job_config = bigquery.LoadJobConfig(
            schema=table_schema.schema,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
        )
        load_job = self.client.load_table_from_uri(uri, table_id, job_config=job_config)