from .main import get_conn, select_data


def build_recordset_sql(schema_name='', include_base_tables = False):
    try: 
        base_query = ("SELECT t.TABLE_SCHEMA as [schema]",
                      "t.TABLE_NAME as recordset_name,",
                      "t.TABLETYPE AS recordset_type",
                      "FROM INFORMATION_SCHEMA.TABLES t",
                      "WHERE table_schema not in ('dbo', 'tSQLt', 'DBA', 'meta', 'stg')")
        base_query = " ".join(base_query)
        if schema_name != '':
            predicate_sql = "AND t.TABLE_SCHEMA = {}".format(schema_name)
            base_query = "{bq} {ps}".format(bq=base_query, ps=predicate_sql)
        if include_base_tables is False:
            second_predicate = "AND t.TABLE_TYPE = 'view'"
            base_query = "{bq} {sp}".format(base_query, second_predicate)
        return(base_query)
            

    except Exception as e:
        print("An error happened in build_recordset_sql(): {}".format(e.args[0]))
        raise

def list_recordsets(schema_name='', include_base_tables=False):
    """Return a list of tables and views in Elmer
    
    """
    try:
       conn = get_conn()
       query = build_recordset_sql()
       df = select_data(query, conn)
       return(df)
    
    except Exception as e:
        print("An error happened in list_recordsets(): {}".format(e.args[0]))
        raise