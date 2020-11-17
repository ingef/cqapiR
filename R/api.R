

### concept requests ###

#' Get concepts from conquery
#'
#' Returns list with concepts of dataset specified in connection
#'
#' @param connection connection object
#' @param include_struc_elements when TRUE, structure elements of concepts
#' in frontend are included in concept list
#' @return concepts of specific dataset in conquery
#' @example man/examples/get_concepts.R
#' @export
get_concepts <- function(connection, include_struc_elements = FALSE){
  connection = add_concepts_end(connection)
  resp = get(connection)
  concepts = httr::content(resp, as="parsed")$concepts

  if (!include_struc_elements){
    concepts = concepts[lapply(concepts, function(x) x[["active"]]) == TRUE]
  }

  return(concepts)
}

#' Get concept from conquery
#'
#' Returns concept with given concept_id
#'
#' @param connection connection object
#' @param concept_id concept id (always includes -dataset- in front,
#' which has to match dataset in connection)
#' @return concept object
#' @export
get_concept <- function(connection, concept_id){
  validate_id_dataset(connection, concept_id)

  connection = add_concept_id_end(connection, concept_id)
  resp = get(connection)
  return(httr::content(resp, as="parsed"))
}

### query requests ###

#' Get query information from query_id
#'
#' Returns query information of query with given query_id
#'
#' @param connection connection object
#' @param query_id query id (always includes -dataset- in front,
#' which has to match dataset in connection)
#' @return query information
#' @export
get_query_info <- function(connection, query_id){
  validate_id_dataset(connection, query_id)

  connection = add_query_id_end(connection, query_id)
  resp = get(connection)

  return(httr::content(resp, as="parsed"))
}


#' Get query from query_id
#'
#' Returns query of query with given query_id
#'
#' @param connection connection object
#' @param query_id query id (always includes -dataset- in front,
#' which has to match dataset in connection)
#' @return query
#' @export
get_query <- function(connection, query_id){
  validate_id_dataset(connection, query_id)

  connection = add_stored_query_id_end(connection, query_id)
  resp = get(connection)
  query_info = httr::content(resp, as="parsed")
  return(query_info$query)
}


#' Get all stored queries
#'
#' Returns all stored queries that are save on the dataset in conncetion
#'
#' @param connection connection object
#' @return list of stored queries
#' @export
get_stored_queries <- function(connection){
  connection = add_stored_queries_end(connection)
  resp= get(connection)
  return(httr::content(resp, as="parsed"))
}

#' Execute query
#'
#' Executes query and returns query_id
#'
#' @param connection connection object
#' @param query query that will be executed
#' @return query
#' @example man/examples/create_and_execute_query.R
#' @export
execute_query <- function(connection, query){
  connection = add_queries_end(connection)
  resp = post(connection, json_data = query)
  return(httr::content(resp, as="parsed")$id)
}

#' Get result of executed query
#'
#' Returns data of executed query in data.table or raw text format
#'
#' @param connection connection object
#' @param query_id query_id of the executed query
#' @param data_format when 'data.table' it returns data in a data.table.
#' If 'raw', data will be returns as csv text
#' @return query result
#' @example man/examples/create_and_execute_query.R
#' @export
get_query_result <- function(connection, query_id, data_format="data.table"){
  query_info = get_query_info(connection, query_id)

  if(query_info$status != "DONE"){
    stop(paste0("Query Status: ", query_info$status))
  }

  connection$url = query_info$resultUrl
  resp = get(connection)

  if (data_format == "data.table"){
    return(data.table::fread(httr::content(resp, "text")))
  } else if (data_format == "raw"){
    return(httr::content(resp, "text"))
  } else {
    stop("Unknown data_format")
  }

  return(data)
}


### query functionality ###

#' Matches query label to query id
#'
#' Looks for label in all stored queries and returns query id if label is found
#'
#' @param connection connection object
#' @param query_label label to query
#' @return query id that belongs to label
#' @export
query_label_to_id <- function(connection, query_label){
  queries = get_stored_queries(connection)
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



#' Creates query from concept id
#'
#' Takes either concept (from concepts list - get_concepts function) or takes
#' connection to get the concept by itself and returns a query.
#' A date range can be added.
#'
#' @param concept_id concept id of concept to turn into query
#' @param connection connection object (only necessary when concept is NULL)
#' @param concept concept that is turned into query
#' @param start_date start date, if date restriction is wanted
#' @param end_date end date, if date restriction is wanted
#' @return query
#' @example man/examples/create_and_execute_query.R
#' @export
concept_to_query <- function(concept_id, connection=NULL, concept=NULL,
                             start_date=NULL, end_date=NULL){

  if (is.null(concept)){
    if (is.null(connection)){
      stop("connection must be defined when concept is NULL")
    }
    validate_id_dataset(connection, concept_id)
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


#' Creates query from concept id
#'
#' Takes concept id and connection to create query from concept
#' A date range can be added.
#'
#' @param connection connection object
#' @param concept_id concept id of concept to turn into query
#' @param start_date start date, if date restriction is wanted
#' @param end_date end date, if date restriction is wanted
#' @return query
#' @example man/examples/create_and_execute_query.R
#' @export
concept_id_to_query <- function(connection, concept_id,
                                start_date=NULL, end_date=NULL){


  validate_id_dataset(connection, concept_id)
  # get root concept
  concepts = get_concepts(connection)

  root_concept_id = root_of_concept_id(concept_id)
  root_concept = concepts[[root_concept_id]]

  # if not root concept, get id from concept_obj
  if (concept_id != root_concept_id){
    root_concept_obj = get_concept(connection, root_concept_id)
    concept_obj = root_concept_obj[[concept_id]]
    if (is.null(concept_obj)){
      stop(paste("Could not find concept_id",
                 concept_id,"as child of", root_concept_id))
    }
    label = concept_obj$label
  } else {
    label = root_concept$label
  }


  tables = list()
  for (table in root_concept$tables){
    tables = list.append(tables, list(id=table$connectorId))
  }

  query = list(
    "type"="CONCEPT",
    "ids"=list(concept_id),
    "label"=label,
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

#' adds date restriction to query
#'
#' adds date restriction to a object of type 'CONCEPT' or 'CONCEPT_QUERY'
#'
#' @param query that will get the date restriction
#' @param start_date start date of date restriction in format: YYYY-MM-DD
#' @param end_date end date of date restriction in format: YYYY-MM-DD
#' @return query with date restriction
#' @example man/examples/date_restriction.R
#' @export
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




#' Create table export query
#'
#' Creates table export query for getting raw data in conquery
#'
#' @param connection connection object
#' @param query_id query_id of the population
#' @param start_date start date for date restriction of table data
#' @param end_date end date for date restriction of table data
#' @param connector_id id of connector that contains raw data
#' @param date_column column name of column that is checked for date restriction
#' @return table export query
#' @example man/examples/wide_table_query.R
#' @export
table_export_query <- function(connection,
                               query_id,
                               start_date,
                               end_date,
                               connector_id,
                               date_column){

  query = get_query(connection, query_id)

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





