### basic request functionality ###

#' Creates connection object
#'
#' The connection object stores url to conquery, credentials and dataset
#'
#' @param dataset dataset to access
#' @param url url to conquery
#' @param token token to access dataset (use login function to add credentials)
#' @return connection object
#' @example man/examples/get_connection.R
#' @export
get_connection <- function(dataset, url=NULL, token=NULL){
  if (is.null(url)){ url = "http://lyo-peva01.spectrumk.ads:8080"}
  return(list(
    dataset=dataset,
    url=url,
    token=token
  ))
}


#' Change dataset
#'
#' Changes dataset in connection to access different dataset
#'
#' @param connection connection obj
#' @param dataset dataset to access
#' @return connection object
#' @export
change_dataset <- function(connection, dataset){
  connection$dataset= dataset
  return(connection)
}

#' Validates connection
#'
#' Checks if there is permission to access any dataset
#'
#' @param connection connection obj
#' @param dataset dataset to access
#' @export
validate_connection <- function(connection){
  connection <- add_datasets_end(connection)
  resp = get(connection)
  dataset_obj_list = httr::content(resp, as="parsed")
  dataset_found = FALSE

  datasets_with_permission = lapply(dataset_obj_list, function(x) x[["id"]])
  datasets_labels_with_permission = lapply(dataset_obj_list, function(x) paste0(x[["id"]], " (", x[["label"]], ")"))


  if (!connection$dataset %in% datasets_with_permission) {
    stop(paste0("No Permission found on dataset ", connection$dataset, ".\n",
                "Did you mean: ",
                paste(datasets_labels_with_permission, collapse = ", ")))
  } else {
    print(paste("User has permission on dataset", connection$dataset))
    print(paste("All datasets with permission:",
                paste(datasets_labels_with_permission, collapse = ", ")))
  }
}



### authentication ###

#' Log in with log in data
#'
#' log in with user name and password to update connection
#'
#' @param connection connection object
#' @return connection object with credential
#' @export
login <- function(connection, user = NULL){
  if(is.null(user)){
    print("Eingabe Benutzername")
    user = getPass::getPass()
  }

  print("Eingabe Passwort")
  password = getPass::getPass()

  connection = add_auth_end(connection)
  auth_resp = post(connection, list("user" = user,
                                    "password" = password))

  # get token from body
  connection$token = httr::content(auth_resp, as="parsed")$access_token

  return(connection)
}
