# psrcelmerpy: 
### *make database connections easier*

## Installation
Install with:
```python
pip install git+https://github.com/psrc/psrcelmerpy.git
```

If you had installed previously and need to update to the current version:
```python
pip install --force-reinstall git+https://github.com/psrc/psrcelmerpy.git
```

## Examples

### Establish a connection to Elmer
```python
import psrcelmerpy
e_conn = psrcelmerpy.ElmerConn()
```

### Establish a connection to ElmerGeo
```python
import psrcelmerpy
eg_conn = psrcelmerpy.ElmerGeoConn()
```

### Retrieve a Table from Elmer (and view the results)
```python
e_conn = psrcelmerpy.ElmerConn()
df = e_conn.get_table(schema='ofm', table_name='publication_dim')
print(df)
```


### Run an ad-hoc query in Elmer 
```python
df_count = e_conn.get_query('select count(*) as rec_count from ofm.publication_dim')
print(df_count)
```

### Retrieve a geodataframe based on a feature class from ElmerGeo
In this example we are using layer "micen" (Manufacturing/Industrial Centers)
```python
eg_conn = psrcelmerpy.ElmerGeoConn()
gdf = eg_conn.read_geolayer('micen')
```

### List all the feature classes available in ElmerGeo:
```python
eg_conn = psrcelmerpy.ElmerGeoConn()
f_classes = eg_conn.list_feature_classes()
```
