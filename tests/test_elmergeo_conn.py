
# from psrcelmerpy.list_feature_classes import (build_feature_class_filter, 
#                                               build_feature_dataset_filter, 
#                                               list_feature_classes,
#                                               build_fc_query)
import psrcelmerpy
#from datetime import datetime
#import pandas as pd
econn = psrcelmerpy.ElmerConn()
egconn = psrcelmerpy.ElmerGeoConn()

# def test_build_featureclass_filter():
#     sql_str = build_feature_class_filter()
#     valid_str = ''
#     assert sql_str == valid_str
#     sql_str = build_feature_class_filter('some_featureclass')
#     valid_str = " AND d.PhysicalName = 'ElmerGeo.DBO.some_featureclass'"
#     assert sql_str == valid_str


# def test_build_feature_dataset_filter():
#     sql_str = build_feature_dataset_filter()
#     valid_str = ''
#     assert sql_str == valid_str
#     sql_str = build_feature_dataset_filter('some_feature_dataset')
#     valid_str = " AND o.[Name] = 'ElmerGeo.DBO.some_feature_dataset'"
#     assert sql_str == valid_str

def test_build_fc_query():
    sql_str = egconn.build_fc_query()
    valid_base = ("select "
      	"replace(d.PhysicalName, 'ELMERGEO.DBO.', '') as layer_name, "
      	"replace(o.[Name],'ElmerGeo.DBO.', '') as feature_dataset, "
      	"d.Definition.value('(/DEFeatureClassInfo/ShapeType/node())[1]', 'nvarchar(max)') as geometry_type "
      "from SDE.GDB_ITEMS o "
        "	left join  SDE.GDB_ITEMRELATIONSHIPS rel on rel.OriginID = o.UUID "
        "	left join SDE.GDB_ItemTypes o_types on o.[Type] = o_types.UUID "
        "	left join SDE.GDB_ITEMS d on rel.DestID = d.UUID "
        "	left join SDE.GDB_ItemTypes d_types on d.[Type] = d_types.UUID "
      "where "
        "	d_types.[name] = 'Feature Class' "
        "	and o_types.[name] = 'Feature Dataset' ")
    valid_str = "".join(valid_base)
    valid_order = " order by o.[Name], d.[name]"
    valid_str = valid_str + valid_order
    assert sql_str == valid_str 

    f_class_filter = " AND d.PhysicalName = 'ElmerGeo.DBO.some_fc'"
    f_dataset_filter = " AND o.[Name] = 'ElmerGeo.DBO.some_fd'"
    valid_base += f_class_filter
    valid_base += f_dataset_filter
    valid_base += valid_order
    valid_str = "".join(valid_base)
    sql_str = egconn.build_fc_query(feature_dataset = 'some_fd', feature_class='some_fc')
    assert sql_str == valid_str 

def test_list_feature_classes():
    df = egconn.list_feature_classes()
    print(f"len(feature clases) = {len(df)}")
    assert len(df) >= 280
    df = egconn.list_feature_classes('census')
    assert 40 < len(df) < 100
