#ifndef RECORD_FILE_HPP
#define RECORD_FILE_HPP

#include "data.hpp"
#include "utils.hpp"

namespace Index {
  template <typename KEY_TYPE> class Record {
  public:
    Data<KEY_TYPE> data;

    physical_pos raw_pos;
    physical_pos dup_pos;

    void setData(Data<KEY_TYPE> _data) { this->data = _data; }

    void setRawPos(physical_pos _raw_pos) { this->raw_pos = _raw_pos; }

    void setDupPos(physical_pos _dup_pos) { this->dup_pos = _dup_pos; }
  };
}


#endif // RECORD_FILE_HPP
