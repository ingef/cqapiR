library(httr)
library(getPass)
library(jsonlite)
library(data.table)


### util functionality ###

list.append <- function(x, to_append){
  x[[length(x)+1]] = to_append
  return(x)
}

dataset_from_id <- function(query_id){
  return(unlist(strsplit(query_id, "\\."))[1])
}

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


### change url in connection to desired endpoint ### 
add_auth_end <- function(connection){
  connection$url <- url_paste(connection$url, "auth")
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

### basic request functionality ###

post <- function(connection, json_data){
  resp = POST(connection$url, 
              add_headers(Authorization=paste0("Bearer ",connection$token)), 
              body=json_data, encode = "json")
  stop_for_status(resp)
  return(resp)
}
get <- function(connection){
  resp = GET(connection$url, 
             add_headers(Authorization=paste0("Bearer ",connection$token)))
  stop_for_status(resp)
  return(resp)
}

### authentication ###
login <- function(connection, user = NULL){
  if(is.null(user)){
    print("Eingabe Benutzername")
    user = getPass()
  }
  
  print("Eingabe Passwort")
  password = getPass()
  
  connection = add_auth_end(connection)
  auth_resp = post(connection, list("user" = user,
                                    "password" = password))
  
  # get token from body
  connection$token = content(auth_resp, as="parsed")$access_token 
  
  return(connection)
}

### concept requests ###
get_concepts <- function(connection, remove_struc_elements = TRUE){
  connection = add_concepts_end(connection)
  resp = get(connection)
  concepts = content(resp, as="parsed")$concepts
  
  if (remove_struc_elements){
    for (concept_id in names(concepts)){
      if (!concepts[[concept_id]][["active"]]){
        concepts[[concept_id]] <- NULL
      }
    }
  }
  return(concepts)
}

get_concept <- function(connection, concept_id){
  connection = add_concept_id_end(connection, concept_id)
  resp = get(connection)
  return(content(resp, as="parsed")[[concept_id]])
}

### query requests ###

get_query_info <- function(connection, query_id){
  
  connection = add_query_id_end(connection, query_id)
  resp = get(connection)
  
  return(content(resp, as="parsed"))
}

get_query <- function(connection, query_id){
  connection = add_stored_query_id_end(connection, query_id)
  resp = get(connection)
  query_info = content(resp, as="parsed")
  return(query_info$query)
}


get_stored_queries <- function(connection){
  connection = add_stored_queries_end(connection)
  resp= get(connection)
  return(content(resp, as="parsed"))
}

execute_query <- function(connection, query){
  connection = add_queries_end(connection)
  resp = post(connection, json_data = query)
  return(content(resp, as="parsed")$id)
}

get_query_result <- function(connection, query_id, data_format="data.table"){
  query_info = get_query_info(connection, query_id)
  
  if(query_info$status != "DONE"){
    stop(paste0("Query Status: ", query_info$status))
  }
  
  connection$url = query_info$resultUrl
  resp = get(connection)
  
  if (data_format == "data.table"){
    return(fread(content(resp, "text")))
  } else if (data_format == "raw"){
    return(content(resp, "text"))
  } else {
    stop("Unknown data_format")
  }
  
  return(data)
}


### query functionality ###
query_label_to_id <- function(connection, query_label){
  queries = get_stored_queries(connection$dataset)
  query_id = NULL
  for (query in queries){
    if(query$label == query_label){
      query_id = query$id
      break
    }
  }
  if (is.null(query_id)){
    stop(paste0("No query found with label", query_label))
  }
  return(query_id)
}


concept_to_query <- function(concept_id, connection=NULL, concept=NULL, 
                             start_date=NULL, end_date=NULL){
  dataset = dataset_from_id(concept_id)
  if (is.null(concept)){
    if (is.null(connection)){
      stop("connection must be defined when concept is NULL")
    }
    concepts = get_concepts(connection)
    concept = concepts[[concept_id]]
  }
  
  tables = list()
  for (table in concept$tables){
    tables = list.append(tables, list(id=table$connectorId))
  }
  
  query = list(
    "type"="CONCEPT",
    "ids"=list(concept_id),
    "label"=concept$label,
    "tables"=tables
  )
  
  if (!is.null(start_date) & !is.null(end_date)){
    return(add_date_restriction(query, start_date, end_date))
  } else {
    return(
      list(
        type="CONCEPT_QUERY",
        root= query
      )
    )
  }
}


add_date_restriction <- function(query, start_date, end_date){
  if (query$type == "CONCEPT_QUERY"){
    base_query = query$root
  } else {
    if (query$type == "CONCEPT"){
      base_query = query
    } else {
      stop("can only put date restriction in queries of type CONCEPT_QUERY")    
    }
  }
  
  if (base_query$type != "CONCEPT"){
    stop("Query of type CONCEPT_QUERY has root that is not of type CONCEPT")
  }
  date_restriction_query = list(
    type= "DATE_RESTRICTION",
    dateRange= list(
      min = start_date,
      max = end_date
    ),
    child = base_query
  )
  return(
    list(
      type="CONCEPT_QUERY",
      root = date_restriction_query
    )
  )
}


absolute_form_query <- function(query_id, start_date, end_date, features, resolution='COMPLETE'){
  return(list(
    type="EXPORT_FORM",
    queryGroup=query_id,
    resolution=resolution,
    timeMode=list(
      value="ABSOLUTE",
      dateRange=list(
        min=start_date,
        max=end_date
      ),
      features=features
    )
  ))
}

relative_form_query <- function(query_id, 
                                features, outcomes, 
                                resolution='COMPLETE',
                                time_unit = 'QUARTERS', 
                                time_count_before = 1, time_count_after = 1,
                                index_selector = 'EARLIEST', 
                                index_placement = 'BEFORE'){
  return(list(
    type="EXPORT_FORM",
    queryGroup=query_id,
    resolution=resolution,
    timeMode=list(
      value="RELATIVE",
      timeUnit=time_unit,
      timeCountBefore=time_count_before,
      timeCountAfter=time_count_after,
      indexSelector=index_selector,
      indexPlacement=index_placement,
      features=features,
      outcomes=outcomes
    )
  ))
}

table_export_query <- function(query,
                               start_date,
                               end_date,
                               connector_id,
                               date_column){
  return(list(
    type="TABLE_EXPORT",
    query=query,
    dateRange=list(
      min=start_date,
      max=end_date
    ),
    tables=list(
      list(
        id=connector_id,
        dateColumn=list(
          value=date_column
        )
      )
    )
  ))
}





