
#' SQLite3 Interface
#'
#' @param file A filename or :memory: for an in-memory database.
#' @param con A connection opened using [gpkg_open()]
#' @param sql An SQL statement
#' @param ptype A `data.frame()` to be used as a prototype for the
#'   result.
#' @param schema A target schema to use as the intended target types
#' @param max_guess The maximum number of rows to use to guess the output
#'   type.
#'
#' @export
#'
gpkg_open <- function(file = ":memory:") {
  if (!identical(file, ":memory:")) {
    file <- path.expand(file)
  }

  structure(
    gpkg_cpp_open(file),
    file = file,
    class = "gpkg_con"
  )
}

#' @rdname gpkg_open
#' @export
gpkg_close <- function(con) {
  gpkg_cpp_close(con)
}

#' @rdname gpkg_open
#' @export
gpkg_exec <- function(con, sql) {
  stopifnot(inherits(con, "gpkg_con"))
  sql <- paste(sql, collapse = ";")
  gpkg_cpp_exec(con, sql)
}

#' @rdname gpkg_open
#' @export
gpkg_guess_schema <- function(con, sql, max_guess = 1000) {
  schema <- nanoarrow::nanoarrow_allocate_schema()
  gpkg_cpp_guess_schema(con, sql, max_guess, schema);
  schema
}

#' @rdname gpkg_open
#' @export
gpkg_query <- function(con, sql, ptype = NULL, max_guess = 1000) {
  if (is.null(ptype)) {
    schema <- NULL
  } else {
    schema <- nanoarrow::infer_nanoarrow_schema(nanoarrow::as_nanoarrow_array(ptype))
  }

  tibble::as_tibble(
    gpkg_query_nanoarrow(con, sql, schema = schema, max_guess = max_guess)
  )
}

#' @rdname gpkg_open
#' @export
gpkg_query_nanoarrow <- function(con, sql, schema = NULL, max_guess = 1000) {
  stopifnot(inherits(con, "gpkg_con"))

  sql <- as.character(sql)

  schema_copy <- nanoarrow::nanoarrow_allocate_schema()

  if (!is.null(schema)) {
    nanoarrow::nanoarrow_pointer_export(
      nanoarrow::as_nanoarrow_schema(schema),
      schema_copy
    )
  } else {
    schema_copy <- gpkg_guess_schema(con, sql, max_guess)
  }

  array <- nanoarrow::nanoarrow_allocate_array()
  gpkg_cpp_query(con, sql, schema_copy, array)

  nanoarrow:::nanoarrow_array_set_schema(array, schema_copy)
  array
}

#' @rdname gpkg_open
#' @export
gpkg_query_table <- function(con, sql, schema = NULL, max_guess = 1000) {
  arrow::as_record_batch(gpkg_query_nanoarrow(con, sql, schema, max_guess = max_guess))
}

#' @rdname gpkg_open
#' @export
gpkg_open_test <- function() {
  con <- gpkg_open()
  gpkg_exec(
    con,
    c(
      "CREATE TABLE crossfit (exercise text,difficulty_level int)",
      "insert into crossfit values ('Push Ups', 3), ('Pull Ups', 5) , ('Push Jerk', 7), ('Bar Muscle Up', 10)"
    )
  )

  con
}

#' @rdname gpkg_open
#' @export
gpkg_list_tables <- function(con) {
  gpkg_query(
    con,
    "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%';",
    ptype = data.frame(name = character(), stringsAsFactors = FALSE)
  )$name
}
