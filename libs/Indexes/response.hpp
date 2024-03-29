#ifndef RESPONSE_HPP
#define RESPONSE_HPP

#include "utils.hpp"

namespace Index {

struct Response {
  std::vector<physical_pos> records;
  std::chrono::milliseconds query_time;

  Response() {}

  void startTimer() { start_time = std::chrono::high_resolution_clock::now(); }

  void stopTimer() {
    auto end_time = std::chrono::high_resolution_clock::now();
    query_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
  }

private:
  std::chrono::high_resolution_clock::time_point start_time;
};

} // namespace Index

#endif // RESPONSE_HPP
