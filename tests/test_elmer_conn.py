# from psrcelmerpy.main import (get_table, get_query, list_recordsets, sql_execute)
# from psrcelmerpy.conn.elmer_conn import ElmerConn
# from psrcelmerpy.conn.elmergeo_conn import ElmerGeoConn
import psrcelmerpy
from datetime import datetime
import pandas as pd
from pytest import raises

def test_get_table():
    econn = psrcelmerpy.ElmerConn()
    # df = get_table(schema='small_areas', tbl_name='sector_dim')
    df = econn.get_table(schema='small_areas', table_name='sector_dim')
    assert len(df) > 1
    assert isinstance(df, pd.DataFrame) == True
    
def test_get_query():
    econn = psrcelmerpy.ElmerConn()
    sql = 'select top 2 * from small_areas.sector_dim'
    df = econn.get_query(sql=sql)
    assert len(df) == 2
    assert isinstance(df, pd.DataFrame) == True
    
def test_list_recordsets():
    econn = psrcelmerpy.ElmerConn()
    df = econn.list_recordsets(schema_name='HHSurvey')
    assert isinstance(df, pd.DataFrame) == True
    assert len(df) > 2
    df = econn.list_recordsets(schema_name='faa', include_base_tables=True)
    assert len(df) == 3

def test_execute_sql():
    econn = psrcelmerpy.ElmerConn()
    thisdate = datetime.today().strftime('%Y_%m_%d')
    tblname = 'stg.test_tbl_psrcelmerpy_' + thisdate
    econn.execute_sql('drop table if exists {}'.format(tblname))
    econn.execute_sql('create table {} (FieldA varchar(10))'.format(tblname))
    econn.execute_sql("insert into {} (FieldA) values ('foo')".format(tblname))
    df = econn.get_table(schema='stg', table_name='test_tbl_psrcelmerpy_' + thisdate)
    assert isinstance(df, pd.DataFrame) == True
    assert len(df) == 1
    econn.execute_sql('drop table if exists {}'.format(tblname))

def test_build_recordset_sql():
    econn = psrcelmerpy.ElmerConn() 
    valid_query = ("SELECT t.TABLE_SCHEMA as [schema],",
                "t.TABLE_NAME as recordset_name,",
                "t.TABLE_TYPE AS recordset_type",
                "FROM INFORMATION_SCHEMA.TABLES t",
                "WHERE table_schema not in ('dbo', 'tSQLt', 'DBA', 'meta', 'stg')",
                "AND t.TABLE_TYPE = 'view'")
    valid_query = " ".join(valid_query)
    test_query = econn._build_recordset_sql()
    assert valid_query == test_query  
    valid_query = f"{valid_query} AND t.TABLE_SCHEMA = 'someschema'"
    valid_query = ("SELECT t.TABLE_SCHEMA as [schema],",
                "t.TABLE_NAME as recordset_name,",
                "t.TABLE_TYPE AS recordset_type",
                "FROM INFORMATION_SCHEMA.TABLES t",
                "WHERE table_schema not in ('dbo', 'tSQLt', 'DBA', 'meta', 'stg')",
                "AND t.TABLE_SCHEMA = 'someschema'",
                "AND t.TABLE_TYPE = 'view'")
    valid_query = " ".join(valid_query)
    test_query = econn._build_recordset_sql(schema_name='someschema')
    assert valid_query == test_query  

def test_elmer_conn_database_name_setter_error():
    con = psrcelmerpy.ElmerConn()
    with raises(ValueError) as exc_info:
        con.database_name=1
    assert str(exc_info.value) == "database_name must be a string"