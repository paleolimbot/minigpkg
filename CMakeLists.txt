# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.11)

project(MiniGPKG)

find_package(SQLite3 REQUIRED)

option(MINIGPKG_CODE_COVERAGE "Enable coverage reporting" OFF)
add_library(coverage_config INTERFACE)

include_directories(src)
add_library(minigpkg src/minigpkg/nanoarrow_sqlite3.c src/minigpkg/nanoarrow.c)
target_link_libraries(minigpkg PUBLIC SQLite::SQLite3)

if (MINIGPKG_BUILD_TESTS)
  set(MEMORYCHECK_COMMAND_OPTIONS "--leak-check=full --error-exitcode=1")
  include(CTest)
  include(FetchContent)

  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)

  find_package(Arrow REQUIRED)

  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/release-1.11.0.zip
  )
  FetchContent_MakeAvailable(googletest)

  enable_testing()

  add_executable(nanoarrow_sqlite3_test src/minigpkg/nanoarrow_sqlite3_test.cc)

  if(MINIGPKG_CODE_COVERAGE)
      target_compile_options(coverage_config INTERFACE -O0 -g --coverage)
      target_link_options(coverage_config INTERFACE --coverage)
      target_link_libraries(minigpkg coverage_config)
  endif()

  target_link_libraries(nanoarrow_sqlite3_test minigpkg arrow_shared gtest_main)

  include(GoogleTest)
  gtest_discover_tests(nanoarrow_sqlite3_test)
endif()
