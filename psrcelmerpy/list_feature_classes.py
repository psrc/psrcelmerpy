from .main import get_conn, select_data

def build_feature_class_filter(feature_class = ''):
    """Build a SQL predicate to filter by feature_class
    
    Parameters
    ----------
    feature_class : str
        The feature class name by which to filter the results.
        
    Returns
    -------
    filter : str
        A clause to add to the predicate of a SQL query
    """
    try:
        filter = ""
        if feature_class != "":
            filter += " AND d.PhysicalName = 'ElmerGeo.DBO.{}'".format(feature_class)
        return(filter)
        
    except Exception as e:
        print("An error happened in build_feature_class_filter(): {}".format(e.args[0]))
        raise


def build_feature_dataset_filter(feature_dataset = ''):
    """Build a SQL predicate to filter by feature_dataset
    
    Parameters
    ----------
    feature_dataset : str
        The feature dataset name by which to filter the results.
        
    Returns
    -------
    filter : str
        A clause to add to the predicate of a SQL query
    """
    try:
        filter = ""
        if feature_dataset != "":
            filter += " AND o.[Name] = 'ElmerGeo.DBO.{}'".format(feature_dataset)
        return(filter)
        
    except Exception as e:
        print("An error happened in build_feature_dataset_filter(): {}".format(e.args[0]))
        raise


def build_fc_query(feature_dataset='', feature_class=''):
    """Build a query to list feature classes in a geodatabase.

    Parameters
    ----------
    feature_dataset : str
         If supplied, the feature dataset to filter the results by (Default value = '')
    feature_class : str
         If supplied, the feature class name to filter the results by (Default value = '')

    Returns
    -------
    sql : str
        A SQL query that can be run against a geodatabase

    """
    try:
        #moremore get 
        feature_class_filter = build_feature_class_filter(feature_class)
        feature_dataset_filter = build_feature_dataset_filter(feature_dataset)
        sql = ("select "
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
        sql += feature_class_filter
        sql += feature_dataset_filter
        sql += " order by o.[Name], d.[name]"
        sql = "".join(sql)
        return(sql)

    except Exception as e:
        print("An error happened in build_fc_query(): {}".format(e.args[0]))
        raise


def list_feature_classes(feature_dataset='', feature_class=''):
    """List the feature clases available in ElmerGeo

    Parameters
    ----------
    feature_dataset : str
        If supplied, this filters the list to just those feature classes in the feature dataset. (Default value = '')
    feature_class : str
        If supplied, this filters the list to just the named one.  
        This can be useful if you know the ame of the feature clas but not the geography type 
        or the feature dataset that it resides in.(Default value = '')

    Returns
    -------
    df : a pandas dataframe listing the layer names along withthecorresponding feature datasets and geometry types.
    """
    try:
        conn = get_conn('ElmerGeo')
        sql = build_fc_query(feature_dataset=feature_dataset, feature_class=feature_class)
        df = select_data(sql=sql, conn=conn)
        conn.dispose()
        return(df)
        

    except Exception as e:
        print("An error happened in list_feature_classes(): {}".format(e.args[0]))
        raise