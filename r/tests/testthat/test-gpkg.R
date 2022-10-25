
test_that("sqlite3 connections can be opened and closed", {
  con <- gpkg_open(":memory:")
  expect_s3_class(con, "gpkg_con")
  gpkg_close(con)
})

test_that("gpkg_exec() executes SQL", {
  con <- gpkg_open()
  on.exit(gpkg_close(con))

  expect_identical(
    gpkg_exec(con, "CREATE TABLE crossfit (exercise text, difficulty_level int)"),
    0L
  )
  expect_error(gpkg_exec(con, "not sql"), "syntax error")
})

test_that("gpkg_open_test() returns a connection", {
  expect_s3_class(gpkg_open_test(), "gpkg_con")
})

test_that("gpkg_query() errors for invalid SQL", {
  con <- gpkg_open_test()
  on.exit(gpkg_close(con))

  expect_error(gpkg_query(con, "not sql"), "SQL logic error")
})

test_that("gpkg_query() can read to data.frame", {
  con <- gpkg_open_test()
  on.exit(gpkg_close(con))

  expect_identical(
    gpkg_query(con, "SELECT * from crossfit"),
    data.frame(
      exercise = c("Push Ups", "Pull Ups", "Push Jerk", "Bar Muscle Up"),
      difficulty_level = c(3, 5, 7, 10),
      stringsAsFactors = FALSE
    )
  )
})

test_that("gpkg_query_table() can read to Table", {
  con <- gpkg_open_test()
  on.exit(gpkg_close(con))

  expect_identical(
    as.data.frame(as.data.frame(gpkg_query_table(con, "SELECT * from crossfit"))),
    data.frame(
      exercise = c("Push Ups", "Pull Ups", "Push Jerk", "Bar Muscle Up"),
      difficulty_level = c(3L, 5L, 7L, 10L),
      stringsAsFactors = FALSE
    )
  )
})

test_that("gpkg_list_tables() lists tables", {
  con <- gpkg_open()
  expect_identical(gpkg_list_tables(con), character())
  gpkg_close(con)

  con <- gpkg_open_test()
  on.exit(gpkg_close(con))
  expect_identical(gpkg_list_tables(con), "crossfit")
})
