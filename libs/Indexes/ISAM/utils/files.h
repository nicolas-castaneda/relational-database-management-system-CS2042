//
// Created by renat on 9/10/2023.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_FILES_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_FILES_H

#include <filesystem>
#include <string>

#if defined(_WIN64)
#include <direct.h>
#elif defined(__unix__)
#include <sys/stat.h>
#include <sys/types.h>
#endif

bool directory_exists(const std::string &path) {
  return std::filesystem::is_directory(path);
}

void createDir(std::string dir) {
#if defined(_WIN64)
  _mkdir(dir.data());
#elif defined(__unix__)
  mkdir(dir.data(), 0777);
#endif
}

/*
 * Reference:
 * https://github.com/ByJuanDiego/disk-static-hash/blob/d8ea2e257cf58e862a983d455062eb1b214145a5/utils/file_utils.h
 * https://stackoverflow.com/questions/23427804/cant-find-mkdir-function-in-dirent-h-for-windows
 *
 */

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_FILES_H
