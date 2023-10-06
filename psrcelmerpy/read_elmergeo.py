from .main import get_conn, select_data
import geopandas as gpd

def build_sql(schema_name, tbl_name, conn):
    """Build a SQL query to select all the columns in table or view {schema_name}.{tbl_name}.
    
    If any columns are of type [geography] or [geography], get its WKT representation
    and name that column [Shape]

    Parameters
    ----------
    schema_name : str
        The schema that the table sits within
    tbl_name: str
        The name of the table or view from which you want to select
    conn : SQLAlchemy connectable str, or sqlite3 connection
        A connection to an SDE database

    Returns
    -------
    ret_str : str
        A query that defines a recordset with a geometry or geography column
    
    """

    try: 
        col_name_ns_sql = ("select c.COLUMN_NAME",
                "from INFORMATION_SCHEMA.COLUMNS c",
                "where c.TABLE_SCHEMA = '{}'".format(schema_name),
                    "and c.TABLE_NAME = '{}'".format(tbl_name),
                    "and c.DATA_TYPE not in ('geometry', 'geography')",
                    "and c.COLUMN_NAME not in ('GDB_GEOMATTR_DATA')"
                    )
        col_name_ns_sql = ' '.join(col_name_ns_sql)
        col_df = select_data(col_name_ns_sql, conn)
        col_names_ns = ', '.join(col_df['COLUMN_NAME'])
        s_col_name_sql = ("select c.COLUMN_NAME + '.STAsText() as Shape' as Column_Name",
                "from INFORMATION_SCHEMA.COLUMNS c",
                "where c.TABLE_SCHEMA = '{}'".format(schema_name),
                    "and c.TABLE_NAME = '{}'".format(tbl_name),
                    "and c.DATA_TYPE in ('geometry', 'geography')",
                    "and c.COLUMN_NAME not in ('GDB_GEOMATTR_DATA')"
                    )
        sf_col_name_sql = ' '.join(s_col_name_sql)
        s_col_df = select_data(s_col_name_sql, conn)
        col_names_s = ', '.join(s_col_df['Column_Name'])
        ret_str = "SELECT  {col_names_ns}, {col_names_s} FROM {schema_name}.{tbl_name}".format(
            col_name_ns=col_names_ns,
            col_name_s=col_names_s,
            schema_name=schema_name,
            tbl_name=tbl_name
        )
        return(ret_str)
    
    except Exception as e:
        print("An error happened in build_sql(): {}".format(e.args[0]))
        raise


def is_table_or_view(layer_name, schema_name, conn, as_evw=False):
    """Verify that a layer exists as either a table or view in the database.
    
    Parameters
    ----------
    layer_name : str
        The name of a geospatial layer or feature class
    schema_name : str
        The schema that the table sits within
    conn : SQLAlchemy connectable str, or sqlite3 connection
        A connection to an SDE database
    as_evw : bool
        A flag indicating that the layer is a versioned view (Default value = False)

    Returns
    -------
    ret_val : bool

    
    """
    try:
        suffix = '_evw' if as_evw else ''
        infoschm_tbl = 'VIEWS' if as_evw else 'TABLES'
        info_schema_sql = ("SELECT TABLE_NAME",
                        "FROM INFORMATION_SCHEMA.{} as v".format(infoschm_tbl),
                        "WHERE v.TABLE_NAME = '{}{}'".format(
                            layer_name, suffix),
                        "   AND v.TABLE_SCHEMA = '{}'".format(schema_name)
                        )
        info_schema_sql = ' '.join(info_schema_sql)
        info_schema_df = select_data(info_schema_sql, conn)
        ret_val = True if len(info_schema_df) > 0 else False
        return(ret_val)
        
    
    except Exception as e:
        print("An error happened in is_table_or_view(): {}".format(e.args[0]))
        raise


def layer_type(layer_name, schema_name, conn):
    """Determine the type of layer (A versioned view, a nonversioned table, or neither)

    Parameters
    ----------
    layer_name : str
        The name of the layer to check
    schema_name: str
        The schema name that the layer exists in in the database
    conn : SQLAlchemy connectable str, or sqlite3 connection
        A connection to an SDE database

    Returns
    -------
    table_type : str
        one of three values ['evw' | 'nonversioned' | 'none']
    
    """
    try:
        if is_table_or_view(layer_name, schema_name, conn, as_evw=True):
            table_type = 'evw'
        elif is_table_or_view(layer_name, schema_name, conn, as_evw=False):
            table_type = 'nonversioned'
        else:
            table_type = 'none'
        return(table_type)
    
    except Exception as e:
        print("An error happened in layer_type(): {}".format(e.args[0]))
        raise


def reproject_gdf(gdf, out_epsg):
    try:
        out_gdf = gdf.to_crs(epsg=out_epsg)
        return(out_gdf)
    
    except Exception as e:
        print("An error happened in reproject_gdf(): {}".format(e.args[0]))
        raise

def read_elmergeo(layer_name, schema_name='dbo', project_to_wgs84=True):
    """ create a geodataframe from a layer in PSRC's in-house geodatabase.

    If the layer has been set up as an ESRI versioned layer in the geodatabase, 
    this function returns the versioned view (which exists in SQL Server with 
    a "_evw" suffix).  If it has not been set up that way, it returns the base table.

    Parameters
    ----------
    layer_name : str
        The name of the feautre layer or geodatabase table
    schema_name : str
        The name of the schema that layer_name exists in.  (Default value = 'dbo')
    project_to_wgs84 : bool
        If True then deliver the output in WGS84 projection, otherwise NAD84 / WA State Plane North.  (Default value = True)

    Returns
    -------
    gdf : A geodataframe
    
    """

    try:
        conn = get_conn('ElmerGeo')
        layr_type = layer_type(layer_name, schema_name, conn)
        if layr_type == 'evw':
            tbl_name = "{}_evw".format(layer_name)
        elif layr_type == 'nonversioned':
            tbl_name = layer_name 
        elif layr_type == 'none':
            raise ValueError("no layer error")
        layer_sql = build_sql(schema_name=schema_name,
                             tbl_name=tbl_name,
                             conn=conn)
        crs='EPSG:2285'
        gdf = gpd.read_postgis(layer_sql, conn, geom_col="Shape", crs=crs)
        if project_to_wgs84:
            gdf.to_crs(epsg='EPSG:4326', inplace=True)
        return(gdf)

    except Exception as e:
        print("An error happened in read_elmergeo(): {}".format(e.args[0]))
        raise