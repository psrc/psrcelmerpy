from .connection import Connection
import pandas as pd
import geopandas as gpd
from shapely import wkt

class ElmerGeoConn(Connection):
    
    def __init__(self):
        """
        Establish a connection to ElmerGeo.
        """
        try:
            self.database_name = 'ElmerGeo'
            self.create_engine()
        
        except Exception as e:
            print(e.args[0])
            raise


    def list_feature_classes(self, feature_dataset='', feature_class=''):
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
            engine = self.engine
            sql = self.build_fc_query(feature_dataset=feature_dataset, feature_class=feature_class)
            df = self.get_query(sql)
            return(df)
            

        except Exception as e:
            print("An error happened in list_feature_classes(): {}".format(e.args[0]))
            raise


    def build_fc_query(self, feature_dataset='', feature_class=''):
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
            feature_class_filter = f" AND d.PhysicalName = 'ElmerGeo.DBO.{feature_class}'"
            feature_dataset_filter = f" AND o.[Name] = 'ElmerGeo.DBO.{feature_dataset}'"
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
            sql += feature_class_filter if feature_class != "" else ""
            sql += feature_dataset_filter if feature_dataset != "" else ""
            sql += " order by o.[Name], d.[name]"
            sql = "".join(sql)
            return(sql)

        except Exception as e:
            print("An error happened in build_fc_query(): {}".format(e.args[0]))
            raise


    def read_geolayer(self, layer_name, schema_name='dbo', project_to_wgs84=True):
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
            # engine = self.engine
            layr_type = self.find_layer_type(layer_name, schema_name)
            if layr_type == 'evw':
                tbl_name = "{}_evw".format(layer_name)
            elif layr_type == 'nonversioned':
                tbl_name = layer_name 
            elif layr_type == 'none':
                raise ValueError("no layer error")
            layer_sql = self.build_feature_class_sql(schema_name=schema_name,
                                tbl_name=tbl_name)
            crs='EPSG:2285'
            gdf = self.sql_to_gdf(layer_sql)
            gdf = gdf.set_crs(crs)
            if project_to_wgs84:
                gdf.to_crs('EPSG:4326', inplace=True)
            return(gdf)

        except Exception as e:
            print("An error happened in read_elmergeo(): {}".format(e.args[0]))
            raise


    def find_layer_type(self, layer_name, schema_name):
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
            if self.is_table_or_view(layer_name, schema_name, as_evw=True):
                table_type = 'evw'
            elif self.is_table_or_view(layer_name, schema_name, as_evw=False):
                table_type = 'nonversioned'
            else:
                table_type = 'none'
            return(table_type)
        
        except Exception as e:
            print("An error happened in layer_type(): {}".format(e.args[0]))
            raise


    def is_table_or_view(self, layer_name, schema_name, as_evw=False):
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
            info_schema_df = self.get_query(info_schema_sql)
            ret_val = True if len(info_schema_df) > 0 else False
            return(ret_val)
            
        except Exception as e:
            print("An error happened in is_table_or_view(): {}".format(e.args[0]))
            raise


    def build_feature_class_sql(self, schema_name, tbl_name):
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
            col_df = self.get_query(col_name_ns_sql)
            col_names_ns = ', '.join(col_df['COLUMN_NAME'])
            s_col_name_sql = ("select c.COLUMN_NAME + '.STAsText() as Shape' as Column_Name",
                    "from INFORMATION_SCHEMA.COLUMNS c",
                    "where c.TABLE_SCHEMA = '{}'".format(schema_name),
                        "and c.TABLE_NAME = '{}'".format(tbl_name),
                        "and c.DATA_TYPE in ('geometry', 'geography')",
                        "and c.COLUMN_NAME not in ('GDB_GEOMATTR_DATA')"
                        )
            s_col_name_sql = ' '.join(s_col_name_sql)
            s_col_df = self.get_query(s_col_name_sql)
            col_names_s = ', '.join(s_col_df['Column_Name'])
            ret_str = "SELECT  {col_names_ns}, {col_names_s} FROM {schema_name}.{tbl_name}".format(
                col_names_ns=col_names_ns,
                col_names_s=col_names_s,
                schema_name=schema_name,
                tbl_name=tbl_name
            )
            return(ret_str)
        
        except Exception as e:
            print("An error happened in build_sql(): {}".format(e.args[0]))
            raise


    def sql_to_gdf(self, sql):
        """Create a geodataframe from a SQL query
        
        The SQL must define a Shape column that is a WKT representation of a geometry data type
        
        """
        try: 
            # df = pd.read_sql(sql, self.engine)
            df = self.get_query(sql)
            df['geometry'] = df['Shape'].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, geometry='geometry')
            return(gdf)

        
        except Exception as e:
            print("An error happened in sql_to_gdf(): {}".format(e.args[0]))
            raise
