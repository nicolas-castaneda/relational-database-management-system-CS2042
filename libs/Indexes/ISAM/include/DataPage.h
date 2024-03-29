//
// Created by renat on 9/7/2023.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_DATAPAGE_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_DATAPAGE_H

#include "../utils/buffer.h"
#include "ISAMRecord.h"
#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

using POS_TYPE = std::streampos;

template <typename KeyType> class DataPage {
private:
  std::vector<ISAMRecord<KeyType>> records;
  POS_TYPE next;
  POS_TYPE count;

public:
  DataPage();

  KeyType getKey(POS_TYPE pos) const { return records[pos].key; }

  bool isFull() const;

  POS_TYPE getCount() const;

  std::vector<ISAMRecord<KeyType>> getRecords() const;

  void setRecord(ISAMRecord<KeyType>, int64_t pos);

  ISAMRecord<KeyType> &getRecord(int64_t pos);

  POS_TYPE getNext() const;

  void setNext(POS_TYPE next);

  static POS_TYPE getCapacity();

  POS_TYPE write(std::fstream &file);

  void read(std::fstream &file);

  static int64_t size_of();

  void remove(int64_t i);

  ISAMRecord<KeyType> last();

  void clearRecord(POS_TYPE pos);

  void insertRecord(ISAMRecord<KeyType> record);

  bool isEmpty();

  ~DataPage();
};

extern template class DataPage<int>;
extern template class DataPage<float>;

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_DATAPAGE_H
