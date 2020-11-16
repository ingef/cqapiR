
# Create and execute simple query

# Step 1. Define concept to use
#         (you can look up all possible ids using get_concepts())
concept_id = "adb_energie.alter"

# Step 2. Create query from concept id and store query in variable
#         Similar to dragging concept in editor field in frontend
query = concept_to_query(concept_id, connection = connection, start_date="2017-01-01", end_date="2017-01-31")

# Step 3. Execute query you created and store query id in variable
query_id = execute_query(connection, query)

# Step 4. Get query result (by default a data.table format is returned)
data = get_query_result(connection, query_id)
