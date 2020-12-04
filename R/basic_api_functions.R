#' post request for conquery
#'
#' makes post request with credentials and url from connection
#' @param connection connection object
#' @param json_data data to send
#' @return response of request
post <- function(connection, json_data){

  resp = httr::POST(connection$url,
                    httr::add_headers(Authorization=paste0("Bearer ",connection$token),
                                      `Content-Type`="application/json"),
                    body=rjson::toJSON(query), encode = "json")

  httr::stop_for_status(resp, httr::content(resp, "text"))

  return(resp)
}

#' get request for conquery
#'
#' makes get request with credentials and url from connection
#' @param connection connection object
#' @return response of request
get <- function(connection){
  header = httr::add_headers(Authorization=paste0("Bearer ",connection$token))
  resp = httr::GET(connection$url, header)
  httr::stop_for_status(resp, httr::content(resp, "text"))
  return(resp)
}
