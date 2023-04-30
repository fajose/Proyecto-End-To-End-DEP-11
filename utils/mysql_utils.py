from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import pandas as pd

class MysqlConnection:
    def __init__(self, ip, port, dbname):
        self.ip = ip
        self.port = port
        self.dbname = dbname 
        
        self.mysql_engine = None
        self.mysql_conn = None   
        
    def set_mysql_params(self,ip, port, dbname):
        self.ip = ip
        self.port = port
        self.dbname = dbname         
    
    def connect_mysql(self):
        self.mysql_engine = create_engine(f"mysql://root:root@{self.ip}:{self.port}/{self.dbname}")
        self.mysql_conn = self.mysql_engine.connect()
        
    def close_mysql_connection(self):
        if self.mysql_conn:
            self.mysql_conn.close()
            self.mysql_engine = None
            self.mysql_conn = None
        else:
            raise Exception('Se requiere una conexión activa de mysql.')
            
    def read_mysql(self, tablename):
        if self.mysql_conn:
            df = pd.read_sql_query(text(f'SELECT * FROM {tablename}'), con=self.mysql_conn)
            return df
        else:
            raise Exception('Se requiere una conexión activa de mysql.')
            
    def get_all_mysql_tables(self):
        if self.mysql_engine:
            inspector = inspect(self.mysql_engine)
            return inspector.get_table_names()
        else:
            raise Exception('Se requiere una conexión activa de mysql.')