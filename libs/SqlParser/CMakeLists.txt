cmake_minimum_required(VERSION 3.26)

project(
  SqlParser
  VERSION 1.0
  DESCRIPTION "An sql parser made in c++"
  LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS on)

# Bison needs to generate headers before linkeage can ocurr
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
find_package(RapidJSON REQUIRED)

bison_target(parser parser.yy ${CMAKE_CURRENT_BINARY_DIR}/parser.tab.cc)
# DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/parser.tab.hh)
flex_target(lexer lexer.l ${CMAKE_CURRENT_BINARY_DIR}/lexer.yy.cc)

add_flex_bison_dependency(lexer parser)

add_library(SqlParser SqlParser.cpp ${BISON_parser_OUTPUTS}
                      ${FLEX_lexer_OUTPUTS})

target_compile_features(SqlParser PUBLIC cxx_std_20)

target_include_directories(SqlParser SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(
  SqlParser
  PRIVATE ${CMAKE_SOURCE_DIR}/include/DBengine
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# target_compile_options( SqlParser PUBLIC # Prefered warnings
# $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX> $<$<NOT:$<CXX_COMPILER_ID:MSVC>>: #[[
# -Werror ]] -Wall -Wextra -Wpedantic -Wformat=2 -Wshadow -Wwrite-strings
# -Wcast-qual -Wcast-align -Wconversion> # Disable some warnings #
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-c++20-compat>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-zero-as-null-pointer-constant> # Solves
# a # bug, # forgot # which one
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-unused-but-set-variable>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-switch-enum>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-padded>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-error=unused-parameter>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>
# $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize-address-use-after-scope>
# $<$<CONFIG:RELEASE>:-O0> $<$<CONFIG:DEBUG>:-O0> $<$<CONFIG:DEBUG>:-ggdb3>)

target_link_options(
  SqlParser PUBLIC
  $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
