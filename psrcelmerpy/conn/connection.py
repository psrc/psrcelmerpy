from . import auth
import sqlalchemy


def build_conn(database_name):
    try:
        driver_name = 'ODBC Driver 17 for SQL Server'
        server_name = 'AWS-PROD-SQL\Sockeye'
        conn_string = "DRIVER={}; SERVER={}; DATABASE={}; trusted_connection=yes".format(
            driver_name,
            server_name,
            database_name
            )
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn_string)
        return(engine)
    
    except Exception as e:
        print(e.args[0])
        raise
    

def get_conn(database_name):
    """Get a connection to a database
    
    Parameters
    ----------
    database_name : string
                    The name of the database to connect to

    Returns
    -------
    conn
        A read-only connection to the database
    """
    try:
        conn = build_conn(database_name)
        return(conn)
    
    except Exception as e:
        print(e.args[0])
        raise