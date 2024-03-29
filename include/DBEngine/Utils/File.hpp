#ifndef FILES_HPP
#define FILES_HPP

#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>

inline void open_or_create(const std::filesystem::path &file_path) {

  std::fstream file_stream;

  // If file doesn't exists create it
  if (!std::filesystem::exists(file_path) ||
      !std::filesystem::is_regular_file(file_path)) {
    spdlog::info("Creating file: {}", file_path.string());
    file_stream.open(file_path, std::ios::out);
    file_stream.close();
  }

  // Try to open for read and write in binary mode
  file_stream.open(file_path, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_stream.is_open()) {
    spdlog::error("Couldn't open file {} for read/write", file_path.string());
  }
  file_stream.close();
}

#endif // !FILES_HPP
