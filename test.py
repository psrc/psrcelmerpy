import psrcelmerpy
from datetime import datetime

# e_conn = psrcelmerpy.ElmerConn()
# df = e_conn.get_table(schema='ofm', table_name='publication_dim')
# print(df)
econn = psrcelmerpy.ElmerConn()
test_query = econn.build_recordset_sql(schema_name='someschema')
print(test_query)
# import psrcelmerpy

# tc = psrcelmerpy.ElmerConn()
# tc.execute_sql('drop table if exists stg.test_cp_42823 ')
# tc.execute_sql('create table stg.test_cp_42823 (FieldA varchar(10))')
# tc.execute_sql("insert into stg.test_cp_42823 (FieldA) values ('foo'), ('bar')")
# df = tc.get_table(schema='stg', table_name='test_cp_42823')
# print(df)
# df_count = tc.get_query('select count(*) as rec_count from stg.test_cp_42823')
# #print(df_count)s

# recordsets = tc.list_recordsets(schema_name='ofm', include_base_tables=True)
# #print(recordsets)

# sc = psrcelmerpy.ElmerGeoConn()
# f_classes = sc.list_feature_classes()
# print("print feature classes...")
# print(f_classes)

# print("getting spatial layer...")
# gdf = sc.read_geolayer('micen')
# print(gdf)
