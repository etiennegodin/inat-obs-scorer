from ..utils.duckdb import _open_connection
from ..utils import to_Path
from ..pipeline import *
from typing import Literal, Any, Dict, List, Optional, Union, Callable
from abc import abstractmethod
from jinja2 import Template

_RETURN_TYPES = Literal["df", "dict", 'list']

class DataBaseConnection(ClassStep):
    
    def __init__(self, **kwargs):
        """_summary_

        Args:
            db_path (_type_): _description_
        """
        self.name = "db_connection"
        super().__init__(self.name, **kwargs)

    
    def _execute(self, context):
        self.db_path = context.config['paths']['db_path']
        self.con = _open_connection(self.db_path)
        context.con = self.con
        print(f"Initiliazing database connection: {self.db_path}")
        return self.db_path
    
class CreateSchema(ClassStep):

    def __init__(self, name, schema:str = "main", **kwargs):
        """_summary_

        Args:
            name (_type_): _description_
            schema (str, optional): _description_. Defaults to "main".
        """
        super().__init__(name, **kwargs)
        self.name = name
        self.schema = schema
    
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        
        if self.schema is None:
            raise ValueError(f'Not schema provided for {self.name} ')
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            return self.schema

        except Exception as e:
            self.logger.error(f'Error creating schema {self.schema} : \n', e)
            return None
       

class DbQuery(ClassStep):

    def __init__(self, name, query_name:str = None, **kwargs):
        super().__init__(name, **kwargs)
        
        if query_name is None:
            self.query_file_name = name
        else:
            self.query_file_name = query_name
            

    @abstractmethod
    def _execute(self, context: PipelineContext) -> Any:
        """Override this in subclasses for query-based steps"""
        pass
            
    def _get_query_path(self, context):
        query_path = to_Path(self.query_file_name) #trick to get .suffix and .stem
        if query_path.suffix != "sql":
            query_path = query_path.parent / f"{query_path.stem}.sql"
            
        return context.config['paths']['queries_folder'] / query_path  
            


class SqlQuery(DbQuery):

    def __init__(self, name, query_name:str = None, **kwargs):
        """_summary_

        Args:
            name (_type_): _description_
            query_name (str, optional): _description_. Defaults to None.
        """
        super().__init__(name, query_name, **kwargs)
        
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        
        self.query_path = self._get_query_path(context)
        
        with open(self.query_path, 'r') as f:
            sql_query = f.read()
        
        try:
            con.execute(sql_query)
        except Exception as e: 
            self.logger.error(e)
            raise e 
        

class SqlQueryTemplate(DbQuery):

    def __init__(self, name, fields:dict,  query_name:str = None, **kwargs):
        """_summary_

        Args:
            name (_type_): _description_
            fields (dict): _description_
            query_name (str, optional): _description_. Defaults to None.
        """
        self.fields = fields
        super().__init__(name, query_name, **kwargs)
        
    def _execute(self, context):
        con = _open_connection(context.get_step_output("db_connection"))
        
        self.query_path = self._get_query_path(context)
            
        with open(self.query_path, 'r') as f:
            try:
                sql_template = Template(f.read())

            except Exception as e:
                print(f"Error reading sql template : {e}")
            
        sql_query = sql_template.render(self.fields)
        
        try:
            con.execute(sql_query)
        except Exception as e: 
            self.logger.error(e)
            raise e 

            
class DataBaseLoader(ClassStep):
    
    def __init__(self, name,
                 columns:list,
                 from_table:str,
                 limit:int = None,
                 return_type: _RETURN_TYPES = "df",
                 **kwargs):
        
        super().__init__(name, **kwargs)
        if not isinstance(columns, list):
            columns = [columns]
        self.columns = self._join_string(columns)
        self.from_table = from_table
        self.limit = limit
        self.return_type = return_type

    def _execute(self, context):

        con = context.con
        try:
            self.logger.info(f"Getting data from db for {self.name} ")
            df = con.execute(f"""SELECT {self.columns} FROM {self.from_table} ORDER BY {self.columns.split(sep=",")[0]} ASC {f'LIMIT {self.limit}' if self.limit is not None else ''}""").df()
            if self.return_type == 'df':
                return df
            elif self.return_type == 'dict':
                output = {}
                for c in df.columns:
                    output[c] = df[c]
            elif self.return_type == "list":
                return df[self.columns].to_list()
            else:
                raise NotImplementedError(f"Return type {self.return_type} is not implemented")
            
        except Exception as e: 
            self.logger.error(e)
            
    def _join_string(self, items:list)->str:
        return ','.join(str(item) for item in items)

        


        
        


        
        
    
    


