add_library(DBEngine DBEngine.cpp)

target_sources(DBEngine PRIVATE HeapFile.cpp Record/Record.cpp)

target_include_directories(DBEngine PUBLIC ${CMAKE_SOURCE_DIR}/include/DBEngine)
