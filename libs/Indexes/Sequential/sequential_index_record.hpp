#ifndef SEQUENTIAL_INDEX_RECORD_HPP
#define SEQUENTIAL_INDEX_RECORD_HPP

#include "../record.hpp"
#include "sequential_index_utils.hpp"

template <typename KEY_TYPE>
class SequentialIndexRecord : public Index::Record<KEY_TYPE> {

public:
  physical_pos current_pos{};
  file_pos current_file{};

  physical_pos next_pos;
  file_pos next_file{};

  SequentialIndexRecord() = default;

  SequentialIndexRecord(Data<KEY_TYPE> &_data, physical_pos &_raw_pos,
                        physical_pos &_dup_pos, physical_pos &_current_pos,
                        file_pos &_current_file, physical_pos &_next_post,
                        file_pos &_next_file) {
    this->data = _data;
    this->raw_pos = _raw_pos;
    this->dup_pos = _dup_pos;

    this->current_pos = _current_pos;
    this->current_file = _current_file;

    this->next_pos = _next_post;
    this->next_file = _next_file;
  }

  void setCurrent(physical_pos _current_pos, file_pos _current_file) {
    this->current_pos = _current_pos;
    this->current_file = _current_file;
  }

  void setNext(physical_pos _next_pos, file_pos _next_file) {
    this->next_pos = _next_pos;
    this->next_file = _next_file;
  }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const SequentialIndexRecord<KEY_TYPE> &sir) {
    stream << sir.data << " | raw_pos: " << sir.raw_pos
           << " | dup_pos: " << sir.dup_pos
           << " | current_pos: " << sir.current_pos
           << " | current_file: " << sir.current_file
           << " | next_pos: " << sir.next_pos
           << " | next_file: " << sir.next_file;
    return stream;
  }
};

#endif // SEQUENTIAL_INDEX_RECORD_HPP
