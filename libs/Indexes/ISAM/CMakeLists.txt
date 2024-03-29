cmake_minimum_required(VERSION 3.26)
project(Indexed_Sequential_Access_Method)

set(CMAKE_EXPORT_COMPILE_COMMANDS on)

# if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
# add_executable(Indexed_Sequential_Access_Method main.cpp src/ISAM.cpp) else()
add_library(Indexed_Sequential_Access_Method src/ISAM.cpp)
# endif()

target_sources(Indexed_Sequential_Access_Method
               PRIVATE src/DataPage.cpp src/IndexPage.cpp src/Metadata.cpp)

target_link_directories(Indexed_Sequential_Access_Method PRIVATE
                        ${CMAKE_SOURCE_DIR}/include/)
target_compile_features(Indexed_Sequential_Access_Method PRIVATE cxx_std_20)

target_compile_options(
  Indexed_Sequential_Access_Method
  PRIVATE # Prefered warnings
          $<$<CXX_COMPILER_ID:MSVC>:/W4
          /WX>
          $<$<CXX_COMPILER_ID:Clang>:-Weverything>
          # $<$<AND:$<CONFIG:DEBUG>,$<NOT:$<CXX_COMPILER_ID:MSVC>>>:-Werror>
          $<$<CXX_COMPILER_ID:GCC>:-fconcepts-diagnostics-depth=3>
          $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:
          -g
          -Wall
          -Wextra
          -Wpedantic
          -Wformat=2
          -Wshadow
          -Wwrite-strings
          -Wcast-qual
          -Wcast-align
          -Wconversion>
          # Disable some warnings when using clang's Weverything
          $<$<CXX_COMPILER_ID:Clang>:-Wno-c++98-compat>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-c++98-compat-pedantic>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-c++20-compat>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-c++20-extensions>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-c99-extensions>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-zero-as-null-pointer-constant>
          $<$<CXX_COMPILER_ID:Clang>:-Wno-error=padded> #
          $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined> #
          $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize-address-use-after-scope>
          $<$<CONFIG:RELEASE>:-O0>
          $<$<CONFIG:DEBUG>:-O0>
          $<$<CONFIG:DEBUG>:-ggdb3>)

target_link_options(
  Indexed_Sequential_Access_Method PRIVATE
  $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
# add_library(SequentialIndex)
