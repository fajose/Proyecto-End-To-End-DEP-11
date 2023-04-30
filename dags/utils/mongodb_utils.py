from pymongo import MongoClient
import pandas as pd

class MongoDBConnection:
    def __init__(self, mongo_uri, db):
        self.mongodb_uri = mongo_uri
        self.mondodb_dbname = db
        
        self.mongodb_client = MongoClient(self.mongodb_uri)
        self.mongodb_database = self.mongodb_client[self.mondodb_dbname]   
        
    def set_mongodb_params(self, mongo_uri, db):
        self.mongodb_uri = mongo_uri
        self.mondodb_dbname = db
        
    def close_mongodb_connection(self):
        if self.mongodb_client is not None:
            self.mongodb_client.close()
            
            self.mongodb_client = None
            self.mongodb_database = None 
        else:
            raise Exception('Se requiere una conexión activa de MongoDB.')
            
    def read_mongodb_collection(self, collection):
        if self.mongodb_client is not None:
            df = pd.DataFrame(self.mongodb_database[collection].find({}))
            return df
        else:
            raise Exception('Se requiere una conexión activa de MongoDB.')
            
    def get_all_collections(self):
        if self.mongodb_database is not None:
            return self.mongodb_database.list_collection_names()
        else:
            raise Exception('Se requiere una conexión activa de MongoDB.')