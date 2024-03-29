#ifndef CONSTANTS_HPP
#define CONSTANTS_HPP

// This file contins compile time constants and type definitions
#ifdef __clang__
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wglobal-constructors"
#endif

#include <array>
#include <string>

namespace Constants {

// Must be owning std::strings to allow for concatenation
#define BASE_PATH "DB_FILES"
#define TABLES_PATH BASE_PATH "/Tables/"
#define METADATA_PATH BASE_PATH "/Metadata/"
#define INDEX_PATH BASE_PATH "/Indexes/"
#define CSV_PATH "SAMPLE_CSV/"

#define DATA_FILE "data.bin"
#define METADATA_FILE "metadata.bin"

const std::array<std::string, 3> INDEX_TYPES = {"ISAM", "Sequential", "AVL"};

} // namespace Constants

#endif // CONSTANTS_HPP
