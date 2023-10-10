from .conn.connection import get_conn
import pandas as pd


def select_data(sql, conn):
    """ Execute a SELECT query against a database and return the result as a data frame

    Parameters
    ----------
    sql : str
        a select query to be executed against the database specified by conn
        
    conn : SQLAlchemy connectable str, or sqlite3 connection
        

    Returns
    -------
    df : a pandas dataframe
    """
    try:
        df = pd.read_sql(sql=sql, con=conn)
        return(df)

    except Exception as e:
        print("An error happened in select_data(): {}".format(e.args[0]))
        raise


def get_query(sql, db_name='Elmer'):
    """Return a recordset defined by a SELECT query against a named database.

    Parameters
    ----------
    sql : str
        The query in SQL format that defines the recordset.
    db_name : str
        The name of the database you are pulling from.  Defaults to 'Elmer'.

    Returns
    -------
    df : A pandas dataframe
    """

    try:
       conn = get_conn(db_name)
       df = select_data(sql, conn)
       conn.dispose()
       return(df)
       
    except Exception as e:
        print("An error happened in get_query(): {}".format(e.args[0]))
        raise


def get_table(schema, tbl_name, db_name='Elmer'):
    """Return a table or view from a database.

    Parameters
    ----------
    schema : str
        The schema that the table or view lives in.
    tbl_name : str
        The name of the table or view that you wish to retrieve
    db_name : str
        The name of the database you are pulling from.  Defaults to 'Elmer'

    Returns
    -------
    df : A pandas data frame
    
    """
    try:
       conn = get_conn(db_name)
       sql = "select * from {}.{}".format(schema, tbl_name)
       df = select_data(sql, conn)
       conn.dispose()
       return(df)
       
    except Exception as e:
        print("An error happened in get_table(): {}".format(e.args[0]))
        raise


def build_recordset_sql(schema_name, include_base_tables=False):
    """ Returns a query to list the tables and views in a database schema.

    Parameters
    ----------
    schema_name : str
        The name of a schema
        
    include_base_tables : bool
        If false, return only the views (no tables).  False is the default.
        

    Returns
    -------

    """
    try:
        qry = ("select t.TABLE_SCHEMA as [schema], "
              "T.TABLE_NAME as recordset_name, "
              "T.TABLE_TYPE AS recordset_type "
            "from INFORMATION_SCHEMA.TABLES t "
            "where table_schema not in ('dbo', 'tSQLt', 'DBA', 'meta', 'stg')")
        if schema_name != '':
            qry += " and table_schema = '{}'".format(schema_name)
        if include_base_tables:
            qry += " and t.TABLE_TYPE = 'view'"
        return(qry)
        
    except Exception as e:
        print("An error happened in build_recordset_sql(): {}".format(e.args[0]))
        raise


def list_recordsets(schema_name='', include_base_tables=False, db_name='Elmer'):
    """ Return a list of tables and views available in Elmer, PSRC's data warehouse (or an alternate db)

    Parameters
    ----------
    schema_name : str
        If supplied, this limits the returned list to only those tables and views in the schema. 
    include_base_tables : bool
        If false, return only the views (i.e., no tables). (Default value = False)
    db_name : str
        The name of the database to retrieve the list of recordsets from.  (Default value = 'Elmer')

    Returns
    -------
    df : a pandas dataframe listing the tables and/or views
    """
    try:
        conn = get_conn(db_name)
        sql = build_recordset_sql(schema_name, include_base_tables)
        df = select_data(sql=sql, conn=conn)
        conn.dispose()
        return(df)
        
    except Exception as e:
        print("An error happened in list_recordsets(): {}".format(e.args[0]))
        raise


def sql_execute(sql, db_name="Elmer"):
    """

    Parameters
    ----------
    sql :
        
    db_name :
         (Default value = "Elmer")

    Returns
    -------

    """
    try:
        conn = get_conn(db_name)
        conn.execute(sql)
        #conn.commit()
        conn.dispose()

    except Exception as e:
        print("An error happened in list_recordsets(): {}".format(e.args[0]))
        raise
