from .conn.connection import get_conn
import pandas as pd


def select_data(sql, conn):
    try:
        df = pd.read_sql(sql=sql, con=conn)
        return(df)

    except Exception as e:
        print("An error happened in select_data(): {}".format(e.args[0]))
        raise


def get_query(sql, db_name='Elmer'):
    """
    Return a recordset defined by a SELECT query.
    
    Parameters
    ----------
    sql : str
        The query in SQL format that defines the recordset.

    db_name : str
        The name of the database you are pulling from.  Defaults to 'Elmer'.
        
    Returns
    -------
    df
        A data frame representation of the results of the query.
    """

    try:
       conn = get_conn(db_name)
       df = select_data(sql, conn)
       conn.close()
       return(df)
       
    except Exception as e:
        print("An error happened in get_query(): {}".format(e.args[0]))
        raise


def get_table(schema, table_name, db_name='Elmer'):
    """
    Return a table or view from a database.
    
    Parameters
    ----------
    schema : str
        The schema that the table or view lives in.

    table_name : str
        The name of the table or view that you wish to retrieve
        
    db_name : str
        The name of the database you are pulling from.  Defaults to 'Elmer'
        
    Returns
    -------
    df
        A data frame representation of the table/view db_name.schema.table_name 
    """
    try:
       conn = get_conn(db_name)
       sql = "select * from {}.{}".format(schema, table_name)
       df = select_data(sql, conn)
       conn.close()
       return(df)
       
    except Exception as e:
        print("An error happened in get_table(): {}".format(e.args[0]))
        raise


def build_recordset_sql(schema_name, include_base_tables):
    try:
        qry = ("select top 10  t.TABLE_SCHEMA as [schema], "
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
    try:
        conn = get_conn(db_name)
        sql = build_recordset_sql(schema_name, include_base_tables)
        #print(sql)
        df = select_data(sql=sql, conn=conn)
        conn.dispose()
        #conn.close()
        return(df)
        
    except Exception as e:
        print("An error happened in list_recordsets(): {}".format(e.args[0]))
        raise