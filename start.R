source('C:/git/cqapiR/functions.R')





mock_json = list(list(name = "name1", task="task1"), 
                 list(name="name2", task="task2"),
                 list(name="name3", task="task2"))

# get all names in a list
lapply(mock_json, function(x) x[["name"]])

# get elements that contain task2
mock_json[lapply(mock_json, function(x) x[["task"]]) == "task2"]

# define conquery parameters

token_initial_user = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"

connection = list(
  url="http://lyo-peva01.spectrumk.ads:8080",
  token=token_initial_user,
  dataset="adb_energie"
)

validate_connection(connection)







# token can be generated from user name and password via log in
connection = login(connection)

# get concepts loaded
get_concepts(connection)

# get executed queries
get_stored_queries(connection)

# create own query from concept id
concept_id = "adb_energie.alter"
query = concept_to_query(concept_id, connection = connection, start_date="2017-01-01", end_date="2017-01-31")

# execute query, get query information and result
query_id = execute_query(connection, query)
get_query_info(connection, query_id)
get_query_result(connection, query_id)

# execute form-query
age_query = concept_to_query("adb_energie.alter", connection = connection)
gender_query = concept_to_query("adb_energie.geschlecht", connection = connection)

## absolute case
absolute_query <- absolute_form_query(query_id, features = list(age_query$root, gender_query$root), 
                                      start_date="2017-01-01", end_date="2017-01-31")
form_query_id = execute_query(connection, absolute_query)
get_query_result(connection, form_query_id)

## relative case
relative_query <- relative_form_query(query_id, features = list(age_query$root, gender_query$root),
                                      outcomes = list(age_query$root, gender_query$root))
form_query_id = execute_query(connection, relative_query)
get_query_result(connection, form_query_id)

# execute table export  (in development -> 02)
connection = list(
  url="http://lyo-peva02.spectrumk.ads:8080",
  token=token_initial_user,
  dataset="adb_energie"
)
table_query = table_export_query(query=get_query(connection, query_id),
                                 start_date="2017-01-01",
                                 end_date="2017-01-31",
                                 connector_id = "adb_energie.alter.alter",
                                 date_column = "adb_energie.alter.alter.versichertenzeit")
table_query_id = execute_query(connection, table_query)
