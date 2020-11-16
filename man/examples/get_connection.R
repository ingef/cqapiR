
# Step 1. Create a connection
connection <- get_connection("fdb_destatis")

# Step 2. Login with your username and password
## Normal login
connection <- login(connection)

## Login with predefined username
connection <- login(connection, user = "max.knobloch@ingef.de")

# Step 3. Validate login and permission on dataset
validate_connection(connection)

# Change dataset of connection
connection <- change_dataset(connection, "fdb_demo")

