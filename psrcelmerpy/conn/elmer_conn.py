from .connection import Connection
import pandas as pd

class ElmerConn(Connection):
    
    def __init__(self):
        """
        Establish a connection to Elmer.
        """
        try:
            self.database_name = 'Elmer'
            self._create_engine()
        
        except Exception as e:
            print(e.args[0])
            raise

    def _build_recordset_sql(self, schema_name='', include_base_tables = False):
        try: 
            base_query = ("SELECT t.TABLE_SCHEMA as [schema],",
                        "t.TABLE_NAME as recordset_name,",
                        "t.TABLE_TYPE AS recordset_type",
                        "FROM INFORMATION_SCHEMA.TABLES t",
                        "WHERE table_schema not in ('dbo', 'tSQLt', 'DBA', 'meta', 'stg')")
            base_query = " ".join(base_query)
            if schema_name != '':
                predicate_sql = f"AND t.TABLE_SCHEMA = '{schema_name}'"
                base_query = f"{base_query} {predicate_sql}"
            if include_base_tables is False:
                second_predicate = "AND t.TABLE_TYPE = 'view'"
                base_query = f"{base_query} {second_predicate}"
            return(base_query)
                

        except Exception as e:
            print("An error happened in _build_recordset_sql(): {}".format(e.args[0]))
            raise


    def list_recordsets(self, schema_name='', include_base_tables=False):
        """
        Return a list of tables and views in Elmer
        
        Parameters
        ----------
        schema_name : String.  The name of a schema to filter by.

        """
        try:
            engine = self.engine
            query = self._build_recordset_sql(schema_name = schema_name, 
                                             include_base_tables=include_base_tables)
            df = self.get_query(query)
            return(df)
        
        except Exception as e:
            print("An error happened in list_recordsets(): {}".format(e.args[0]))
            raise

    def stage_table(self, df, table_name):
        try:
            engine = self.engine
            df.to_sql(name=table_name, schema='stg', con=engine, index=False)

        except Exception as e:
            print(e.args[0])
            raise