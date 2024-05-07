from . import auth
import sqlalchemy
import pandas as pd

class Connection:

    def __init__(self, database_name):
        try:
            self.database_name = database_name
            self.create_engine()
        
        except Exception as e:
            print(e.args[0])
            raise
    

    def create_engine(self):
        try:
            self.driver_name = 'ODBC Driver 17 for SQL Server'
            self.server_name = r'AWS-PROD-SQL\Sockeye'
            conn_string = "DRIVER={}; SERVER={}; DATABASE={}; trusted_connection=yes".format(
                self.driver_name,
                self.server_name,
                self.database_name
                )
            self.engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn_string)

            
        except Exception as e:
            print(e.args[0])
            raise


    def get_query(self, sql):
        """Return a recordset defined by a SELECT query against a named database.

        Parameters
        ----------
        sql : str
            The query in SQL format that defines the recordset.

        Returns
        -------
        df : A pandas dataframe
        """

        try:
            engine = self.engine
            df = pd.read_sql(sql=sql, con=engine)
            return(df)
        
        except Exception as e:
            print("An error happened in get_query(): {}".format(e.args[0]))
            raise


    def execute_sql(self, sql):
        """

        Parameters
        ----------
        sql : a SQL query

        Returns
        -------
        nothing.  This executes some SQL and quits.

        """
        try:
            engine = self.engine
            engine.execute(sql)
            engine.dispose()

        except Exception as e:
            print("An error happened in connection.execute_sql(): {}".format(e.args[0]))
            raise


    def get_table(self, schema, table_name):
        """
        Return a table or view from a database.
        
        Parameters
        ----------
        schema : str
            The schema that the table or view lives in.

        table_name : str
            The name of the table or view that you wish to retrieve
            
        Returns
        -------
        df
            A data frame representation of the table/view db_name.schema.table_name 
        """
        try:
            engine = self.engine
            sql = "select * from {}.{}".format(schema, table_name)
            print(sql)
            df = self.get_query(sql)
            return(df)
        
        except Exception as e:
            print("An error happened in get_table(): {}".format(e.args[0]))
            raise