table_query = table_export_query(connection = connection,
                                 query_id = query_id,
                                 start_date="2017-01-01",
                                 end_date="2017-01-31",
                                 connector_id = "fdb_destatis.alter.alter",
                                 date_column = "fdb_destatis.alter.alter.versichertenzeit")

table_query_id = execute_query(connection, table_query)
