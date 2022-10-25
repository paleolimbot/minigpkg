#include <cpp11.hpp>
using namespace cpp11;

#include <sstream>

#include "sqlite3.h"

#include "nanoarrow_sqlite3.h"

class GPKGConnection {
public:
  sqlite3* ptr;
  GPKGConnection(): ptr(nullptr) {}

  ~GPKGConnection() {
    if (ptr != nullptr) {
      sqlite3_close(ptr);
    }
  }
};

class GPKGStmt {
public:
  sqlite3_stmt* ptr;
  GPKGStmt(): ptr(nullptr) {}

  ~GPKGStmt() {
    if (ptr != nullptr) {
      sqlite3_finalize(ptr);
    }
  }
};

class SQLite3Result {
public:
  SQLite3Result() { ArrowSQLite3ResultInit(&result_); }
  ArrowSQLite3Result* get() { return &result_; }
  ~SQLite3Result() { ArrowSQLite3ResultReset(&result_); }

private:
  ArrowSQLite3Result result_;
};


class GPKGError: public std::runtime_error {
public:
  GPKGError(const std::string& err): std::runtime_error(err) {}
};



[[cpp11::register]]
cpp11::sexp gpkg_cpp_open(std::string filename) {
  external_pointer<GPKGConnection> con(new GPKGConnection());
  int result = sqlite3_open(filename.c_str(), &con->ptr);
  if (result != SQLITE_OK) {
    stop("%s", sqlite3_errstr(result));
  }

  return as_sexp(con);
}

[[cpp11::register]]
void gpkg_cpp_close(cpp11::sexp con_sexp) {
  external_pointer<GPKGConnection> con(con_sexp);
  int result = sqlite3_close(con->ptr);
  if (result != SQLITE_OK) {
    stop("%s", sqlite3_errstr(result));
  }

  con->ptr = nullptr;
}

[[cpp11::register]]
int gpkg_cpp_exec(cpp11::sexp con_sexp, std::string sql) {
  external_pointer<GPKGConnection> con(con_sexp);
  char* error_message = nullptr;
  int result = sqlite3_exec(con->ptr, sql.c_str(), nullptr, nullptr, &error_message);
  if (error_message != nullptr) {
    stop("%s", error_message);
  }

  return result;
}

[[cpp11::register]]
int gpkg_cpp_query(cpp11::sexp con_sexp, std::string sql,
                   sexp schema_xptr, sexp array_xptr) {
  external_pointer<GPKGConnection> con(con_sexp);
  auto schema = reinterpret_cast<struct ArrowSchema*>(R_ExternalPtrAddr(schema_xptr));
  auto array = reinterpret_cast<struct ArrowArray*>(R_ExternalPtrAddr(array_xptr));

  GPKGStmt stmt;
  SQLite3Result arrow_result;
  int result;

  if (schema->release != nullptr) {
    result = ArrowSQLite3ResultSetSchema(arrow_result.get(), schema);
    if (result != 0) {
      stop("<ArrowSQLite3ResultSetSchema> %s\n",
           ArrowSQLite3ResultError(arrow_result.get()));
    }
  }

  const char* tail;
  result = sqlite3_prepare_v2(con->ptr, sql.data(), sql.size(), &stmt.ptr, &tail);
  if (result != SQLITE_OK) {
    stop("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con->ptr));
  }

  int64_t row_id = 0;
  do {
    result = ArrowSQLite3ResultStep(arrow_result.get(), stmt.ptr);
    if (result != 0) {
      stop("<ArrowSQLite3ResultError on row %ld> %s\n", (long)row_id,
             ArrowSQLite3ResultError(arrow_result.get()));
    }

    row_id++;
  } while (arrow_result.get()->step_return_code == SQLITE_ROW);

  result = ArrowSQLite3ResultFinishSchema(arrow_result.get(), schema);
  if (result != 0) {
    stop("<ArrowSQLite3ResultFinishSchema> %s\n",
         ArrowSQLite3ResultError(arrow_result.get()));
  }

  result = ArrowSQLite3ResultFinishArray(arrow_result.get(), array);
  if (result != 0) {
    stop("<ArrowSQLite3ResultFinishArray> %s\n",
         ArrowSQLite3ResultError(arrow_result.get()));
  }

  return row_id;
}



