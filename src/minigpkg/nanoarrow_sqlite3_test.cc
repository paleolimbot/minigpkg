
#include <stdexcept>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include "nanoarrow_sqlite3.h"

using namespace arrow;

class ConnectionHolder {
 public:
  sqlite3* ptr;
  ConnectionHolder() : ptr(nullptr) {}

  int open_memory() {
    int result = sqlite3_open(":memory:", &ptr);
    if (result != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errstr(result));
    }

    return result;
  }

  int exec(const std::string& sql) {
    char* error_message = nullptr;
    int result = sqlite3_exec(ptr, sql.c_str(), nullptr, nullptr, &error_message);
    if (error_message != nullptr) {
      throw std::runtime_error(error_message);
    }

    return result;
  }

  void add_crossfit_table() {
    exec("CREATE TABLE crossfit (exercise TEXT, difficulty_level INTEGER)");
    exec(
        "INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5), ('Push Jerk', "
        "7), ('Bar Muscle Up', 10), ('Unknown', NULL)");
  }

  ~ConnectionHolder() {
    if (ptr != nullptr) {
      sqlite3_close(ptr);
    }
  }
};

class StmtHolder {
 public:
  sqlite3_stmt* ptr;

  StmtHolder() : ptr(nullptr) {}

  int prepare(sqlite3* con, const std::string& sql) {
    const char* tail;
    int result = sqlite3_prepare_v2(con, sql.c_str(), sql.size(), &ptr, &tail);
    if (result != SQLITE_OK) {
      std::stringstream stream;
      stream << "<" << sqlite3_errstr(result) << "> " << sqlite3_errmsg(con);
      throw std::runtime_error(stream.str().c_str());
    }

    return result;
  }

  ~StmtHolder() {
    if (ptr != nullptr) {
      sqlite3_finalize(ptr);
    }
  }
};

void ASSERT_ARROW_OK(Status status) {
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

TEST(SQLite3Test, SQLite3ResultBasic) {
  struct ArrowSQLite3Result result;
  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);
  ArrowSQLite3ResultReset(&result);
}

TEST(SQLite3Test, SQLite3ResultSetSchema) {
  struct ArrowSQLite3Result result;
  struct ArrowSchema schema;
  schema.release = nullptr;

  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);

  EXPECT_EQ(ArrowSQLite3ResultSetSchema(&result, nullptr), EINVAL);
  EXPECT_STREQ(ArrowSQLite3ResultError(&result), "schema is null or released");
  EXPECT_EQ(ArrowSQLite3ResultSetSchema(&result, &schema), EINVAL);
  EXPECT_STREQ(ArrowSQLite3ResultError(&result), "schema is null or released");

  ASSERT_ARROW_OK(ExportType(*int32(), &schema));
  EXPECT_EQ(ArrowSQLite3ResultSetSchema(&result, &schema), EINVAL);
  EXPECT_STREQ(ArrowSQLite3ResultError(&result), "schema is not a struct");
  schema.release(&schema);

  ASSERT_ARROW_OK(ExportSchema(*arrow::schema({}), &schema));
  EXPECT_EQ(ArrowSQLite3ResultSetSchema(&result, &schema), 0);

  ArrowSQLite3ResultReset(&result);
}

TEST(SQLite3Test, SQLite3ResultWithGuessedSchema) {
  ConnectionHolder con;
  con.open_memory();
  con.add_crossfit_table();

  StmtHolder stmt;
  stmt.prepare(con.ptr,
               "SELECT exercise, difficulty_level, CAST(exercise AS BLOB) as blob_col, "
               "difficulty_level * 1.1 AS float_col from crossfit");

  struct ArrowSQLite3Result result;
  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);

  do {
    EXPECT_EQ(ArrowSQLite3ResultStep(&result, stmt.ptr), 0);
  } while (result.step_return_code == SQLITE_ROW);

  struct ArrowArray array;
  struct ArrowSchema schema;
  EXPECT_EQ(ArrowSQLite3ResultFinishArray(&result, &array), 0);
  EXPECT_EQ(ArrowSQLite3ResultFinishSchema(&result, &schema), 0);

  auto maybe_array = ImportArray(&array, &schema);
  ASSERT_ARROW_OK(maybe_array.status());

  EXPECT_TRUE(maybe_array.ValueUnsafe()->type()->Equals(
      struct_({field("exercise", utf8()), field("difficulty_level", int64()),
               field("blob_col", binary()), field("float_col", float64())})));

  auto arr = std::dynamic_pointer_cast<StructArray>(maybe_array.ValueUnsafe());
  EXPECT_EQ(arr->length(), 5);

  auto col1 = std::dynamic_pointer_cast<StringArray>(arr->field(0));
  EXPECT_EQ(col1->Value(0), "Push Ups");
  EXPECT_EQ(col1->Value(1), "Pull Ups");
  EXPECT_EQ(col1->Value(2), "Push Jerk");
  EXPECT_EQ(col1->Value(3), "Bar Muscle Up");
  EXPECT_EQ(col1->Value(4), "Unknown");

  auto col2 = std::dynamic_pointer_cast<Int64Array>(arr->field(1));
  EXPECT_EQ(col2->Value(0), 3);
  EXPECT_EQ(col2->Value(1), 5);
  EXPECT_EQ(col2->Value(2), 7);
  EXPECT_EQ(col2->Value(3), 10);
  EXPECT_TRUE(col2->IsNull(4));

  ArrowSQLite3ResultReset(&result);
}

TEST(SQLite3Test, SQLite3ResultWithExplicitSchema) {
  ConnectionHolder con;
  con.open_memory();
  con.add_crossfit_table();

  StmtHolder stmt;
  stmt.prepare(con.ptr, "SELECT * from crossfit");

  struct ArrowSQLite3Result result;
  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);

  auto explicit_schema =
      arrow::schema({field("col1", large_utf8()), field("col2", int16())});
  struct ArrowSchema schema_in;
  ASSERT_ARROW_OK(ExportSchema(*explicit_schema, &schema_in));
  ASSERT_EQ(ArrowSQLite3ResultSetSchema(&result, &schema_in), 0);

  do {
    EXPECT_EQ(ArrowSQLite3ResultStep(&result, stmt.ptr), 0);
  } while (result.step_return_code == SQLITE_ROW);

  struct ArrowArray array;
  struct ArrowSchema schema;
  EXPECT_EQ(ArrowSQLite3ResultFinishArray(&result, &array), 0);
  EXPECT_EQ(ArrowSQLite3ResultFinishSchema(&result, &schema), 0);

  auto maybe_array = ImportArray(&array, &schema);
  ASSERT_ARROW_OK(maybe_array.status());

  EXPECT_TRUE(maybe_array.ValueUnsafe()->type()->Equals(
      struct_({field("col1", large_utf8()), field("col2", int16())})));

  auto arr = std::dynamic_pointer_cast<StructArray>(maybe_array.ValueUnsafe());
  EXPECT_EQ(arr->length(), 5);

  auto col1 = std::dynamic_pointer_cast<LargeStringArray>(arr->field(0));
  EXPECT_EQ(col1->Value(0), "Push Ups");
  EXPECT_EQ(col1->Value(1), "Pull Ups");
  EXPECT_EQ(col1->Value(2), "Push Jerk");
  EXPECT_EQ(col1->Value(3), "Bar Muscle Up");
  EXPECT_EQ(col1->Value(4), "Unknown");

  auto col2 = std::dynamic_pointer_cast<Int16Array>(arr->field(1));
  EXPECT_EQ(col2->Value(0), 3);
  EXPECT_EQ(col2->Value(1), 5);
  EXPECT_EQ(col2->Value(2), 7);
  EXPECT_EQ(col2->Value(3), 10);
  EXPECT_TRUE(col2->IsNull(4));

  ArrowSQLite3ResultReset(&result);
}

TEST(SQLite3Test, SQLite3ResultFromEmpty) {
  ConnectionHolder con;
  con.open_memory();
  con.add_crossfit_table();

  StmtHolder stmt;
  stmt.prepare(con.ptr, "SELECT * from crossfit WHERE 0");

  struct ArrowSQLite3Result result;
  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);

  do {
    EXPECT_EQ(ArrowSQLite3ResultStep(&result, stmt.ptr), 0);
  } while (result.step_return_code == SQLITE_ROW);

  struct ArrowArray array;
  struct ArrowSchema schema;
  EXPECT_EQ(ArrowSQLite3ResultFinishArray(&result, &array), 0);
  EXPECT_EQ(ArrowSQLite3ResultFinishSchema(&result, &schema), 0);

  auto maybe_array = ImportArray(&array, &schema);
  ASSERT_ARROW_OK(maybe_array.status());

  EXPECT_TRUE(maybe_array.ValueUnsafe()->type()->Equals(
      struct_({field("exercise", null()), field("difficulty_level", null())})));

  auto arr = std::dynamic_pointer_cast<StructArray>(maybe_array.ValueUnsafe());
  EXPECT_EQ(arr->length(), 0);

  ArrowSQLite3ResultReset(&result);
}

TEST(SQLite3Test, SQLite3ResultAppendError) {
  ConnectionHolder con;
  con.open_memory();
  con.add_crossfit_table();

  StmtHolder stmt;
  stmt.prepare(con.ptr, "SELECT exercise from crossfit");

  struct ArrowSQLite3Result result;
  ASSERT_EQ(ArrowSQLite3ResultInit(&result), 0);

  auto explicit_schema = arrow::schema({field("exercise", int32())});
  struct ArrowSchema schema_in;
  ASSERT_ARROW_OK(ExportSchema(*explicit_schema, &schema_in));
  ASSERT_EQ(ArrowSQLite3ResultSetSchema(&result, &schema_in), 0);

  EXPECT_EQ(ArrowSQLite3ResultStep(&result, stmt.ptr), EINVAL);
  EXPECT_STREQ(ArrowSQLite3ResultError(&result),
               "Row 0, column 0 ('exercise'): \n  Can't append value 'Push Ups' (SQLite "
               "type SQLITE_TEXT) to Arrow type with format 'i'");

  ArrowSQLite3ResultReset(&result);
}
