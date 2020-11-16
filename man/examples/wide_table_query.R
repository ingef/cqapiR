table_query = table_export_query(query=get_query(connection, query_id),
                                 start_date="2017-01-01",
                                 end_date="2017-01-31",
                                 connector_id = "adb_energie.alter.alter",
                                 date_column = "adb_energie.alter.alter.versichertenzeit")
table_query_id = execute_query(connection, table_query)
