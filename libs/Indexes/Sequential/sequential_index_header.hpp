#ifndef SEQUENTIAL_INDEX_HEADER_HPP
#define SEQUENTIAL_INDEX_HEADER_HPP

#include "sequential_index_utils.hpp"

class SequentialIndexHeader {

public:
  physical_pos next_pos = END_OF_FILE;
  file_pos next_file = INDEXFILE;

  SequentialIndexHeader() = default;

  SequentialIndexHeader(physical_pos _next_post, file_pos _next_file)
      : next_pos(_next_post), next_file(_next_file) {}

  void setNext(physical_pos _next_pos, file_pos _next_file) {
    this->next_pos = _next_pos;
    this->next_file = _next_file;
  }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const SequentialIndexHeader &sih) {
    stream << " | next_pos: " << sih.next_pos
           << " | next_file: " << sih.next_file;
    return stream;
  }
};

#endif // SEQUENTIAL_INDEX_HEADER_HPP
