

### util functionality - small helper functions ###



list.append <- function(x, to_append){
  x[[length(x)+1]] = to_append
  return(x)
}

dataset_from_id <- function(query_id){
  return(unlist(strsplit(query_id, "\\."))[1])
}

root_of_concept_id <- function(concept_id){
  concept_id_parts = unlist(strsplit(concept_id, "\\."))
  return(paste(concept_id_parts[1:2], collapse="."))
}
