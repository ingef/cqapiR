### change url in connection to desired endpoint ###

url_paste <- function(...){
  url = ""
  for (url_component in list(...)){
    url_component = sub("^/+", "", url_component)
    url_component = sub("/+$", "", url_component)
    url = paste(url, url_component, sep="/")
    url = sub("^/+", "", url)
  }
  return(url)
}

add_auth_end <- function(connection){
  connection$url <- url_paste(connection$url, "auth")
  return(connection)
}

add_datasets_end <- function(connection){
  connection$url <- url_paste(connection$url, "api", "datasets")
  return(connection)
}

add_dataset_end <- function(connection){
  connection$url <- url_paste(connection$url, "api", "datasets", connection$dataset)
  return(connection)
}

add_concepts_end <- function(connection){
  connection = add_dataset_end(connection)
  connection$url <- url_paste(connection$url, "concepts")
  return(connection)
}

add_concept_id_end <- function(connection, concept_id){
  connection = add_dataset_end(connection)
  connection$url <- url_paste(connection$url, "concept", concept_id)
  return(connection)
}

add_stored_queries_end <- function(connection){
  connection = add_dataset_end(connection)
  connection$url <- url_paste(connection$url, "stored-queries")
  return(connection)
}

add_stored_query_id_end <- function(connection, query_id){
  connection = add_stored_queries_end(connection)
  connection$url <- url_paste(connection$url, query_id)
  return(connection)
}

add_queries_end <- function(connection){
  connection = add_dataset_end(connection)
  connection$url <- url_paste(connection$url, "queries")
  return(connection)
}

add_query_id_end <- function(connection, query_id){
  connection = add_dataset_end(connection)
  connection$url <- url_paste(connection$url, "queries", query_id)
  return(connection)
}
