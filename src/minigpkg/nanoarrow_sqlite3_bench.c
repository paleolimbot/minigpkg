
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nanoarrow_sqlite3.h"

int main(int argc, char* argv[]) {
  struct ArrowArray array;
  struct ArrowSchema schema;
  array.release = NULL;
  schema.release = NULL;

  if (argc < 2) {
    printf("Usage: nanoarrow_sqlite3_bench <filename> <queries ...>\n");
    return 1;
  }

  sqlite3* con = NULL;
  int result = sqlite3_open(argv[1], &con);
  if (result != SQLITE_OK) {
    printf("sqlite3_open_v2(): %s\n", sqlite3_errstr(result));
    return 1;
  }

  for (int i = 2; i < argc; i++) {
    sqlite3_stmt* stmt;
    const char* tail;

    printf("Running query %s\n", argv[i]);

    // Once just looping through every row and column getting each value
    result = sqlite3_prepare_v2(con, argv[i], strlen(argv[i]), &stmt, &tail);
    if (result != SQLITE_OK) {
      printf("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con));
      return 1;
    }

    clock_t start = clock();

    // Maybe enough to keep this loop from optimizing out
    int64_t a_number = 0;

    while ((result = sqlite3_step(stmt)) == SQLITE_ROW) {
      int ncol = sqlite3_column_count(stmt);
      for (int j = 0; j < ncol; j++) {
        switch (sqlite3_column_type(stmt, j)) {
          case SQLITE_BLOB:
            a_number += sqlite3_column_bytes(stmt, i);
            a_number += (intptr_t)sqlite3_column_blob(stmt, i);
            break;
          case SQLITE_TEXT:
            a_number += sqlite3_column_bytes(stmt, i);
            a_number += (intptr_t)sqlite3_column_text(stmt, i);
            break;
          case SQLITE_FLOAT:
            a_number += sqlite3_column_double(stmt, i);
            break;
          case SQLITE_INTEGER:
            a_number += sqlite3_column_int64(stmt, i);
            break;
          case SQLITE_NULL:
            a_number += 1;
            break;
        }
      }
    }

    clock_t end = clock();
    printf("...the magic number is %d\n", (int)(a_number % 5));
    printf("...looped through result in %f seconds\n",
           (end - start) / (double)CLOCKS_PER_SEC);

    if (result != SQLITE_DONE) {
        printf("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con));
        sqlite3_finalize(stmt);
        sqlite3_close(con);
        return 1;
    }

    sqlite3_finalize(stmt);

    // Once building an arrow array
    result = sqlite3_prepare_v2(con, argv[i], strlen(argv[i]), &stmt, &tail);
    if (result != SQLITE_OK) {
      printf("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con));
      sqlite3_finalize(stmt);
      sqlite3_close(con);
      return 1;
    }

    struct ArrowSQLite3Result arrow_result;
    ArrowSQLite3ResultInit(&arrow_result);

    printf("Building Arrow result for query %s\n", argv[i]);
    start = clock();
    do {
      result = ArrowSQLite3ResultStep(&arrow_result, stmt);
      if (result != 0) {
        printf("<ArrowSQLite3ResultError> %s\n", ArrowSQLite3ResultError(&arrow_result));
        sqlite3_finalize(stmt);
        sqlite3_close(con);
        ArrowSQLite3ResultReset(&arrow_result);
      }
    } while (arrow_result.step_return_code == SQLITE_ROW);

    result = ArrowSQLite3ResultFinishArray(&arrow_result, &array);
    end = clock();
    printf("...processed %ld rows in %f seconds\n", (long)array.length,
           (end - start) / (double)CLOCKS_PER_SEC);
    if (array.release != NULL) {
      array.release(&array);
    }

    if (result != 0) {
      printf("<ArrowSQLite3ResultError> %s\n", ArrowSQLite3ResultError(&arrow_result));
      sqlite3_finalize(stmt);
      sqlite3_close(con);
      ArrowSQLite3ResultReset(&arrow_result);
      return 1;
    }

    sqlite3_finalize(stmt);
    ArrowSQLite3ResultReset(&arrow_result);
  }

  sqlite3_close(con);
  return 0;
}
