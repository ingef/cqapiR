
# Contains functionality to validate function input

validate_id_dataset <- function(connection, conquery_id){
  if (dataset_from_id(conquery_id) != connection$dataset){
    stop("Concept_id and connection have different dataset.")
  }
}

validate_resolution <- function(resolution){
  if (!(resolution %in% c('COMPLETE', 'YEARS', 'QUARTERS'))){
    stop("resolution must be either 'COMPLETE', 'YEARS' or 'QUARTERS'")
  }
}

validate_time_unit <- function(resolution){
  if (!(resolution %in% c('QUARTERS', 'DAYS'))){
    stop("resolution must be either 'QUARTERS', 'DAYS'")
  }
}

validate_index_selector <- function(resolution){
  if (!(resolution %in% c('EARLIEST', 'RANDOM', 'LATEST'))){
    stop("resolution must be either 'EARLIEST', 'RANDOM', 'LATEST'")
  }
}

validate_index_placement <- function(resolution){
  if (!(resolution %in% c('BEFORE', 'NEUTRAL', 'AFTER'))){
    stop("resolution must be either 'BEFORE', 'NEUTRAL', 'AFTER'")
  }
}
