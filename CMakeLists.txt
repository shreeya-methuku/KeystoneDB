cmake_minimum_required(VERSION 3.16)
project(keystone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra)

option(KEYSTONE_TSAN "Build with ThreadSanitizer" OFF)
if(KEYSTONE_TSAN)
    add_compile_options(-fsanitize=thread -g)
    add_link_options(-fsanitize=thread)
endif()

add_library(keystone STATIC src/db.cpp src/memtable.cpp src/crc32.cpp src/wal.cpp src/sstable.cpp src/merge_iterator.cpp src/bloom.cpp src/manifest.cpp src/block_cache.cpp)
target_include_directories(keystone PUBLIC include)

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.14.0
)
FetchContent_MakeAvailable(googletest)

include(CTest)
add_executable(keystone_tests
  tests/db_test.cpp
  tests/memtable_test.cpp
  tests/wal_test.cpp
  tests/sstable_test.cpp
  tests/scan_test.cpp
  tests/bloom_test.cpp
  tests/compaction_test.cpp
  tests/manifest_test.cpp
  tests/concurrency_test.cpp
  tests/group_commit_test.cpp)
target_link_libraries(keystone_tests PRIVATE keystone GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(keystone_tests)

add_executable(torture tools/torture.cpp)
target_link_libraries(torture PRIVATE keystone)

add_executable(keystone_cli cmd/keystone/main.cpp)
set_target_properties(keystone_cli PROPERTIES OUTPUT_NAME keystone)
target_link_libraries(keystone_cli PRIVATE keystone)

# The benchmark needs SQLite. It's optional: the core library, tests, CLI, and
# torture test all build without it. Disable with -DKEYSTONE_BENCH=OFF.
option(KEYSTONE_BENCH "Build the SQLite comparison benchmark" ON)
if(KEYSTONE_BENCH)
    find_package(SQLite3)
    if(SQLite3_FOUND)
        if(NOT TARGET SQLite3::SQLite3)
            add_library(SQLite3::SQLite3 IMPORTED INTERFACE)
            set_target_properties(SQLite3::SQLite3 PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${SQLite3_INCLUDE_DIRS}"
                INTERFACE_LINK_LIBRARIES "${SQLite3_LIBRARIES}")
        endif()
        add_executable(bench bench/bench.cpp)
        target_link_libraries(bench PRIVATE keystone SQLite3::SQLite3)
        target_compile_options(bench PRIVATE -O2)
    else()
        message(STATUS "SQLite3 not found — skipping the 'bench' target.")
    endif()
endif()
