from .conn.connection import get_conn
import pandas as pd

def get_table(schema, table_name, db_name='Elmer'):
    """
    Return a table from a database.
    
    Parameters
    ----------
    schema : str
        The schema that the table lives in.

    table_name : str
        The name of the table you wish to retrieve
        
    db_name : str
        The name of the database you are pulling from.  Defaults to 'Elmer'
        
    Returns
    -------
    df
        A data frame representation of the table db_name.schema.table_name 
    """
    try:
       conn = get_conn(db_name)
       sql = "select * from {}.{}".format(schema, table_name)
       print(sql)
       df = pd.read_sql(sql=sql, con=conn)
       return(df)
       
    except Exception as e:
        print(e.args[0])
        raise