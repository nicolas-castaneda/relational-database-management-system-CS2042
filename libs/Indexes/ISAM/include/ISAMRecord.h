//
// Created by renato on 9/23/23.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAMRECORD_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAMRECORD_H

#include <iostream>

using POS_TYPE = std::streampos;

template <typename KeyType> struct ISAMRecord {
  KeyType key;
  POS_TYPE pos = -1;
  POS_TYPE dup_pos = -1;
};
extern template struct ISAMRecord<int>;
extern template struct ISAMRecord<float>;

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_ISAMRECORD_H
