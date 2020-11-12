library(httr)
library(getPass)
library(jsonlite)
library(data.table)


login <- function(user = NULL){
  if(is.null(user)){
    print("Eingabe Benutzername")
    user = getPass()
  }
  
  print("Eingabe Passwort")
  password = getPass()
  
  auth_url = paste0(cq_url, "/auth")
  auth_resp = POST(auth_url, 
                   body=list("user" = user,
                             "password" = password),
                   encode = "json")
  
  # stop when something goes wrong
  stop_for_status(auth_resp)
  
  # get token from body
  token = content(auth_resp, as="parsed")$access_token
  
  return(token)
}

dataset_from_query_id <- function(query_id){
  return(unlist(strsplit(query_id, "\\."))[1])
}

post <- function(url, json_data){
  return(POST(url, add_headers(Authorization=paste0("Bearer ",cq_token)), body=json_data, encode = "json"))
}
get <- function(url){
  return(GET(url, add_headers(Authorization=paste0("Bearer ",cq_token))))
}

execute_saved_query <- function(query_id){
  dataset = unlist(strsplit(query_id, "\\."))[1]
  query = list(
    type="SAVE_QUERY",
    query=query_id
  )
  
}

get_stored_queries <- function(dataset){
  resp = get(paste0(cq_url,"/api/datasets/",dataset,"/stored-queries"))
  stop_for_status(resp)
  return(content(resp, as="parsed"))
}

query_label_to_id <- function(dataset, query_label){
  queries = get_stored_queries(dataset)
  query_id = NULL
  for (query in queries){
    print(query$label)
    i = 0
    if(query$label == query_label){
      print(i)
      i = i+1
      query_id = query$id
      break
    }
  }
  if (is.null(query_id)){
    stop(paste0("No query found with label", query_label))
  }
  return(query_id)
}

execute_query <- function(dataset, query){
  resp = post(paste0(cq_url, "/api/datasets/", dataset, "/queries"), json_data = query)
  stop_for_status(resp)
  return(content(resp, as="parsed")$id)
}


get_query_result <- function(query_id){
  dataset = dataset_from_query_id(query_id)
  query_info = get_query_info(query_id)
  
  if(query_info$status != "DONE"){
    stop(paste0("Query Status: ", query_info$status))
  }
  
  resp = GET(query_info$resultUrl, add_headers(Authorization=paste0("Bearer ",cq_token)))
  
  stop_for_status(resp)
  data = fread(content(resp, "text"))
  return(data)
}

get_query_info <- function(query_id){
  dataset = dataset_from_query_id(query_id)
  resp = GET(paste0(cq_url, "/api/datasets/", dataset, "/queries/", query_id),
             add_headers(Authorization=paste0("Bearer ",cq_token)))
  
  stop_for_status(resp)
  
  return(content(resp, as="parsed"))
}

list.append <- function(x, to_append){
  x[[length(x)+1]] = to_append
  return(x)
}
concept_to_query <- function(concept_id, concept=NULL, 
                             start_date=NULL, end_date=NULL){
  dataset = dataset_from_query_id(concept_id)
  if (is.null(concept)){
    concepts = get_concepts(dataset)
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

get_concepts <- function(dataset, remove_struc_elements = TRUE){
  resp = get(paste0(cq_url, "/api/datasets/", dataset, "/concepts"))
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

get_concept <- function(concept_id){
  dataset = dataset_from_query_id(concept_id)
  resp = get(paste0(cq_url, "/api/datasets/", dataset, "/concepts/", concept_id ))
  return(content(resp, as="parsed")[[concept_id]])
}
cq_url = "http://lyo-peva01.spectrumk.ads:8080"
cq_token = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"


