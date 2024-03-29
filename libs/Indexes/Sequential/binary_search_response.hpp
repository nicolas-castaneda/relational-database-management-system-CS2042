#ifndef BINARY_SEARCH_RESPONSE_HPP
#define BINARY_SEARCH_RESPONSE_HPP

#include "sequential_index_header.hpp"
#include "sequential_index_record.hpp"

enum status { EMPTY_FILE = -2, REC_PREV = -1, REC_CUR = 0, REC_NEXT = 1 };

template <typename KEY_TYPE> struct BinarySearchResponse {
  status location = EMPTY_FILE;
  bool header = false;

  SequentialIndexHeader sih;

  SequentialIndexRecord<KEY_TYPE> sir_prev;
  SequentialIndexRecord<KEY_TYPE> sir;
  SequentialIndexRecord<KEY_TYPE> sir_next;

  BinarySearchResponse() = default;

  void setHeader(SequentialIndexHeader _sih) {
    this->sih = _sih;
    this->header = true;
  }

  void setRecords(SequentialIndexRecord<KEY_TYPE> _sir_prev,
                  SequentialIndexRecord<KEY_TYPE> _sir,
                  SequentialIndexRecord<KEY_TYPE> _sir_next) {
    this->sir_prev = _sir_prev;
    this->sir = _sir;
    this->sir_next = _sir_next;
  }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const BinarySearchResponse<KEY_TYPE> &bsr) {
    stream << "BinarySearchResponse: \n";
    stream << "location: " << bsr.location << "\n";
    stream << "header_bool: " << bsr.header << "\n";
    stream << "header_rec: " << bsr.sih << "\n";
    stream << "sir_prev: " << bsr.sir_prev << "\n";
    stream << "sir: " << bsr.sir << "\n";
    stream << "sir_next: " << bsr.sir_next << "\n";

    return stream;
  }
};

#endif // BINARY_SEARCH_RESPONSE_HPP
