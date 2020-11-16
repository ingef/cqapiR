

### util functionality - small helper functions ###



list.append <- function(x, to_append){
  x[[length(x)+1]] = to_append
  return(x)
}

dataset_from_id <- function(query_id){
  return(unlist(strsplit(query_id, "\\."))[1])
}
