#include <cpp11.hpp>
using namespace cpp11;

#include <sstream>
#include <vector>

#include "sqlite3.h"

#include "nanoarrow.h"
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

static int sqlite_type2(int current_type, int new_type) {
  switch (current_type) {
  case SQLITE_NULL:
    switch (new_type) {
      case SQLITE_NULL:
        return current_type;
      case SQLITE_INTEGER:
      case SQLITE_FLOAT:
      case SQLITE_TEXT:
      case SQLITE_BLOB:
        return new_type;
    }
  case SQLITE_INTEGER:
    switch (new_type) {
      case SQLITE_NULL:
      case SQLITE_INTEGER:
        return current_type;
      case SQLITE_FLOAT:
      case SQLITE_TEXT:
      case SQLITE_BLOB:
        return new_type;
    }
  case SQLITE_FLOAT:
    switch (new_type) {
      case SQLITE_NULL:
      case SQLITE_INTEGER:
      case SQLITE_FLOAT:
        return current_type;
      case SQLITE_TEXT:
      case SQLITE_BLOB:
        return new_type;
    }
  case SQLITE_TEXT:
    switch (new_type) {
      case SQLITE_NULL:
      case SQLITE_INTEGER:
      case SQLITE_FLOAT:
      case SQLITE_TEXT:
        return current_type;
      case SQLITE_BLOB:
        return new_type;
    }
  case SQLITE_BLOB:
    switch (new_type) {
      case SQLITE_NULL:
      case SQLITE_INTEGER:
      case SQLITE_FLOAT:
      case SQLITE_TEXT:
      case SQLITE_BLOB:
        return current_type;
    }
  }

  return current_type;
}

[[cpp11::register]]
void gpkg_cpp_guess_schema(cpp11::sexp con_sexp, std::string sql, double max_guess,
                           sexp schema_xptr) {
  if (max_guess == 0) {
    return;
  }

  external_pointer<GPKGConnection> con(con_sexp);
  auto schema = reinterpret_cast<struct ArrowSchema*>(R_ExternalPtrAddr(schema_xptr));

  GPKGStmt stmt;
  const char* tail;
  int result = sqlite3_prepare_v2(con->ptr, sql.data(), sql.size(), &stmt.ptr, &tail);
  if (result != SQLITE_OK) {
    stop("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con->ptr));
  }

  int64_t row_id = 0;
  int64_t n_cols;
  std::vector<std::string> names;
  std::vector<int> sqlite_types;

  // step once to initialize sqlite_types
  result = sqlite3_step(stmt.ptr);
  row_id++;
  if (result != SQLITE_ROW && result != SQLITE_DONE) {
    stop("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con->ptr));
  }

  n_cols = sqlite3_column_count(stmt.ptr);
  names.resize(n_cols);
  sqlite_types.resize(n_cols);
  for (int64_t i = 0; i < n_cols; i++) {
    names[i] = sqlite3_column_name(stmt.ptr, i);
    sqlite_types[i] = sqlite3_column_type(stmt.ptr, i);
  }

  // step the rest of the times
  while (row_id < max_guess && result == SQLITE_ROW) {
    result = sqlite3_step(stmt.ptr);
    if (result != SQLITE_ROW && result != SQLITE_DONE) {
      stop("<%s> %s\n", sqlite3_errstr(result), sqlite3_errmsg(con->ptr));
    }

    for (int64_t i = 0; i < n_cols; i++) {
      sqlite_types[i] = sqlite_type2(
        sqlite_types[i],
        sqlite3_column_type(stmt.ptr, i)
      );
    }

    row_id++;
  }

  // Calculate the schema
  ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT);
  ArrowSchemaAllocateChildren(schema, n_cols);
  ArrowSchemaSetName(schema, "");

  for (int64_t i = 0; i < n_cols; i++) {
    switch (sqlite_types[i]) {
      case SQLITE_NULL:
        ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_BOOL);
        break;
      case SQLITE_INTEGER:
        ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_INT64);
        break;
      case SQLITE_FLOAT:
        ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_DOUBLE);
        break;
      case SQLITE_TEXT:
        ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_STRING);
        break;
      case SQLITE_BLOB:
        ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_BINARY);
        break;
    }

    ArrowSchemaSetName(schema->children[i], names[i].c_str());
  }
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



