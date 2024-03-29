set(CMAKE_EXPORT_COMPILE_COMMANDS on)

# Include headers for all tests
include_directories(PRIVATE ../include)

macro(package_add_test TESTNAME)
  # create an executable in which the tests will be stored
  add_executable(${TESTNAME} ${ARGN})

  # c++20
  target_compile_features(${TESTNAME} PRIVATE cxx_std_20)

  # link the Google test infrastructure, mocking library, and a default main
  # function to the test executable.  Remove g_test_main if writing your own
  # main function.
  target_link_libraries(
    ${TESTNAME}
    gtest
    gmock
    gtest_main
    SqlParser
    Api
    DBEngine)

  # gtest_discover_tests replaces gtest_add_tests, see
  # https://cmake.org/cmake/help/v3.10/module/GoogleTest.html for more options
  # to pass to it
  gtest_discover_tests(
    ${TESTNAME}
    # set a working directory so your project root so that you can find test
    # data via paths relative to the project root
    WORKING_DIRECTORY ${PROJECT_DIR}
    PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}")
  set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)

endmacro()

package_add_test(apiTest apiTest.cpp)
package_add_test(createTableTest createTable.cpp)
package_add_test(bulkInsertTest bulkInsert.cpp)
package_add_test(bigTest bigTest.cpp)
package_add_test(idxTest createIndex.cpp)
