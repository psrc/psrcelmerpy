from psrcelmerpy.main import (get_table, get_query, list_recordsets, sql_execute)
from datetime import datetime
import pandas as pd

def test_get_table():
    df = get_table(schema='small_areas', tbl_name='sector_dim')
    assert len(df) > 1
    assert isinstance(df, pd.DataFrame) == True
    
def test_get_query():
    sql = 'select top 2 * from small_areas.sector_dim'
    df = get_query(sql=sql)
    assert len(df) == 2
    assert isinstance(df, pd.DataFrame) == True
    
def test_list_recordsets():
    df = list_recordsets(schema_name='HHSurvey')
    assert isinstance(df, pd.DataFrame) == True
    assert len(df) > 2
    df = list_recordsets(schema_name='faa')
    assert len(df) == 3

def test_sql_execute():
    thisdate = datetime.today().strftime('%Y_%m_%d')
    tblname = 'stg.test_tbl_psrcelmerpy_' + thisdate
    sql_execute('drop table if exists {}'.format(tblname))
    sql_execute('create table {} (FieldA varchar(10))'.format(tblname))
    sql_execute("insert into {} (FieldA) values ('foo')".format(tblname))
    df = get_table(schema='stg', tbl_name='test_tbl_psrcelmerpy_' + thisdate)
    assert isinstance(df, pd.DataFrame) == True
    assert len(df) == 1
    sql_execute('drop table if exists {}'.format(tblname))

