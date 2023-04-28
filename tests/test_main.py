from psrcelmerpy.main import (get_table, get_query, list_recordsets)
import pandas as pd

def test_get_table():
    df = get_table(schema='small_areas', table_name='sector_dim')
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
