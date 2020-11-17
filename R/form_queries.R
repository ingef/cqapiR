
###################### Form Queries #######################


#' Creates concept objects from concept ids to use in export form
concept_ids_to_concept_objs <- function(connection, concept_ids){

  concepts = get_concepts(connection)

  needed_root_concept_objs = list()

  queries = list()

  # build query for each concept id
  for (concept_id in concept_ids){

    # get root concept to extract tables (and label if concept is root concept)
    root_concept_id = root_of_concept_id(concept_id)
    root_concept = concepts[[root_concept_id]]
    if (is.null(root_concept)){
      stop(paste("Could not find root",root_concept_id,"for concept id", concept_id))
    }

    # when concept id is child concept, get root concept obj to check if concept id is valid and to extract label
    if (root_concept_id == concept_id){
      label = root_concept$label
    } else {
      if (!root_concept_id %in% names(needed_root_concept_objs)){
        needed_root_concept_objs[[root_concept_id]] = get_concept(connection, root_concept_id)
      }
      root_concept_obj = needed_root_concept_objs[[root_concept_id]][[concept_id]]
      if (is.null(root_concept_obj)){
        stop(paste("Could not find concept id", concept_id))
      }
      label = root_concept_obj$label
    }

    tables = list()
    for (table in root_concept$tables){
      tables = list.append(tables, list(id=table$connectorId))
    }
    queries = list.append(queries,
                          list(
                            "type"="CONCEPT",
                            "ids"=concept_id,
                            "label"=label,
                            "tables"=tables
                          ))
  }
  return(queries)
}



#' Create form query of type 'ABSOLUTE'
#'
#' Creates query of type 'ABSOLUTE'
#'
#' @param query_id query_id for query that defines population
#' @param start_date start date of date restriction for features
#' @param end_date end date of date restriction for features
#' @param queries list of queries
#' (type 'CONCEPT_QUERY' - from create_query function)
#' @param concept_ids list of concept ids to use instead of queries
#' @param connection connection object is needed when using concept_ids
#' @param resolution time resolution of output - 'COMPLETE', 'YEARS', 'QUARTERS'
#' @return absolute form query
#' @example man/examples/form_query.R
#' @export
absolute_form_query <- function(query_id, start_date, end_date, queries = NULL,
                                concept_ids = NULL, connection = NULL,
                                resolution='COMPLETE'){

  if (is.null(queries) & is.null(concept_ids)){
    stop("To create a form query, either queries or concept_ids have to be defined.")
  }

  if (is.null(connection) & !is.null(concept_ids)){
    stop("To create a form query from concept_ids, a connection object is required.")
  }

  if (!is.null(concept_ids)){
    queries = concept_ids_to_concept_objs(connection, concept_ids)
  }

  validate_resolution(resolution)

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
      features=queries
    )
  ))
}

#' Create form query of type 'ABSOLUTE'
#'
#' Creates query of type 'ABSOLUTE'
#'
#' @param query_id query_id for query that defines population
#' @param before_queries list of queries for time before index date
#' (type 'CONCEPT_QUERY' - from create_query function)
#' @param after_queries list of queries for time after index date
#' (type 'CONCEPT_QUERY' - from create_query function)
#' @param before_concept_ids list of concept ids to use instead of before_queries
#' @param after_concept_ids list of concept ids to use instead of after_queries
#' @param connection connection object is needed when using concept_ids
#' @param resolution time resolution of output - 'COMPLETE', 'YEARS', 'QUARTERS'
#' @param time_unit time unit of for before and after date range
#' - 'QUARTERS', 'DAYS'
#' @param time_count_before number of time units in date range before index date
#' @param time_count_after number of time units in date range after index date
#' @param index_selector specifies how the index date will be retrieved
#' from date range per person - 'EARLIEST', 'RANDOM', 'LATEST'
#' @param index_placement specifies if time unit of index date is counted to
#' before date range, after date range or if it lies inbetween.
#'  - 'BEFORE', 'NEUTRAL', 'AFTER'
#' @return relative form query
#' @example man/examples/form_query.R
#' @export
relative_form_query <- function(query_id,
                                before_queries = NULL, after_queries = NULL,
                                before_concept_ids = NULL,
                                after_concept_ids = NULL,
                                connection=NULL,
                                resolution='COMPLETE',
                                time_unit = 'QUARTERS',
                                time_count_before = 1, time_count_after = 1,
                                index_selector = 'EARLIEST',
                                index_placement = 'BEFORE'){

  if (is.null(before_queries) & is.null(before_concept_ids)){
    stop("To create a form query, either before_queries or
         before_concept_ids have to be defined.")
  }

  if (is.null(after_queries) & is.null(after_concept_ids)){
    stop("To create a form query, either after_queries or
         after_concept_ids have to be defined.")
  }

  if (is.null(connection) &
      (!is.null(before_concept_ids) | !is.null(after_concept_ids))){
    stop("To create a form query from concept_ids,
         a connection object is required.")
  }


  # TODO: convert before and after ids in one go to save concept loading when
  # same root has to be loaded for both date ranges
  if (!is.null(before_concept_ids)){
    before_queries = concept_ids_to_concept_objs(connection, before_concept_ids)
  }

  if (!is.null(before_concept_ids)){
    after_queries = concept_ids_to_concept_objs(connection, after_concept_ids)
  }


  validate_resolution(resolution)
  validate_time_unit(time_unit)
  validate_index_selector(index_selector)
  validate_index_placement(index_placement)

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
      features=before_queries,
      outcomes=after_queries
    )
  ))
}
