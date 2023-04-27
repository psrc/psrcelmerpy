from psrcelmerpy.get_table import get_table

def test_get_table():
    df = get_table(schema='small_areas', table_name='sector_dim')
    assert len(df) > 1