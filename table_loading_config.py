table_configs = [
{   'table_name': 'load_test_2',
    'increment_method': 'increment_column',  # increment_column # query
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.mysql_load_test',
    'merge_columns_list': ['test_id']
},
{   'table_name': 'load_test_2',
    'increment_method': 'query',  # increment_column # query
    'query': '''select * from tenjin.load_test_4 ''',
    'increment_column': 'loaded_at',
    'rows_per_increment': 1000*10,
    'schema_name': 'tenjin',
    'bucket': 'datateam_bucket',
    'dataset': 'get-data-team.mysql_load_test',
    'merge_columns_list': ['test_id']
},
 ]



tables_list_column_increment = []

for table in tables_list_column_increment:
    table_configs.append({'table_name': table,
                          'increment_method': 'increment_column', # increment_column # query
                          'increment_column': 'loaded_at',
                          'rows_per_increment': 1000*10,
                          'schema_name': 'tenjin',
                          'bucket': 'datateam_bucket',
                          'dataset': 'get-data-team.tenjin_dv_test',
                          'merge_columns_list': ['test_id']
                         })
