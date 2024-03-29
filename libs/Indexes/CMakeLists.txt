cmake_minimum_required(VERSION 3.16)
project(Indexes)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)

add_library(Indexes INTERFACE)

add_subdirectory(ISAM)

target_link_libraries(Indexes INTERFACE Indexed_Sequential_Access_Method)

target_compile_features(Indexes INTERFACE cxx_std_20)
target_include_directories(Indexes SYSTEM INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)

  add_executable(IndexesTest test/test.cpp)

  target_compile_features(IndexesTest PRIVATE cxx_std_20)

  target_link_libraries(IndexesTest PRIVATE Indexes)

endif()
