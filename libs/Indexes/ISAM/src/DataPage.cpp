//
// Created by renat on 9/7/2023.
//

#include "../include/DataPage.h"
#include "fstream"

template <typename KeyType> DataPage<KeyType>::DataPage() {
  int64_t capacity = this->getCapacity();
  records.reserve(static_cast<unsigned long>(capacity));
  next = -1;
  count = 0;
}

template <typename KeyType>
POS_TYPE DataPage<KeyType>::write(std::fstream &file) {
  POS_TYPE pos = file.tellp();
  int64_t capacity = this->getCapacity();
  // Write count attribute
  file.write((char *)&count, sizeof(count));
  // Write isLeaf attribute
  file.write((char *)&next, sizeof(next));
  // write records
  for (int i = 0; i < capacity; ++i) {
    file.write((char *)&(records[static_cast<unsigned long>(i)]),
               sizeof(ISAMRecord<KeyType>));
  }
  return pos;
}

template <typename KeyType> void DataPage<KeyType>::read(std::fstream &file) {
  int64_t capacity = this->getCapacity();
  // Read count attribute
  file.read((char *)&count, sizeof(count));
  // Read isLeaf attribute
  file.read((char *)&next, sizeof(next));
  // Read records
  for (int i = 0; i < capacity; ++i) {
    file.read((char *)&(records[static_cast<unsigned long>(i)]),
              sizeof(ISAMRecord<KeyType>));
  }
}

template <typename KeyType> int64_t DataPage<KeyType>::size_of() {
  int64_t size = 0;
  POS_TYPE capacity = getCapacity();
  size += sizeof(count) + sizeof(next);
  size += sizeof(ISAMRecord<KeyType>) * static_cast<unsigned long>(capacity);
  return size;
}

/*
 * N: Number of records_pos in ISAM Data Pages
 *             |-----------records-------------|   |-----next-----|
 * |-----count-----| BufferSize = sizeof(ISAMRecord<KeyType> * (N) +
 * sizeof(POS_TYPE) + sizeof(POS_TYPE) (BufferSize - sizeof(POS_TYPE) -
 * sizeof(POS_TYPE)) / sizeof(ISAMRecord<KeyType> = N
 */

template <typename KeyType> POS_TYPE DataPage<KeyType>::getCapacity() {
  //    auto a = (buffer::get_buffer_size() - sizeof(POS_TYPE)*2) /
  //    sizeof(ISAMRecord<KeyType>);

  return 3;
}

template <typename KeyType> bool DataPage<KeyType>::isFull() const {
  return count == getCapacity();
}

template <typename KeyType> POS_TYPE DataPage<KeyType>::getCount() const {
  return count;
}

template <typename KeyType>
std::vector<ISAMRecord<KeyType>> DataPage<KeyType>::getRecords() const {
  return records;
}

template <typename KeyType>
void DataPage<KeyType>::setRecord(ISAMRecord<KeyType> record, int64_t pos) {
  try {
    records[static_cast<unsigned long>(pos)] = record;
    count = count + static_cast<POS_TYPE>(1);
  } catch (std::exception &e) {
    throw(std::runtime_error(
        "Cannot write record with key: " + std::to_string(record.key) +
        " in position: " + std::to_string(pos)));
  }
}

template <typename KeyType>
ISAMRecord<KeyType> &DataPage<KeyType>::getRecord(int64_t pos) {
  try {
    return records[static_cast<unsigned long>(pos)];
  } catch (std::exception &e) {
    throw(std::runtime_error("Cannot access in position: " +
                             std::to_string(pos)));
  }
}

template <typename KeyType> POS_TYPE DataPage<KeyType>::getNext() const {
  return next;
}

template <typename KeyType> void DataPage<KeyType>::setNext(POS_TYPE n) {
  DataPage::next = n;
}

template <typename KeyType> void DataPage<KeyType>::remove(int64_t i) {
  if (i < 0 || i >= getCount())
    throw std::runtime_error("Cannot index a record in position: " +
                             std::to_string(i));
  if (i == getCount() - static_cast<POS_TYPE>(1)) {
    records[static_cast<unsigned long>(i)] = ISAMRecord<KeyType>();
  } else {
    auto j = static_cast<POS_TYPE>(i + 1);
    for (; j < getCount(); j = j + static_cast<POS_TYPE>(1)) {
      setRecord(getRecord(j), j - static_cast<POS_TYPE>(1));
      count = count - static_cast<POS_TYPE>(1);
    }
    records[static_cast<unsigned long>(j - static_cast<POS_TYPE>(1))] =
        ISAMRecord<KeyType>();
  }
  count = count - static_cast<POS_TYPE>(1);
}

template <typename KeyType> ISAMRecord<KeyType> DataPage<KeyType>::last() {
  return records[static_cast<unsigned long>(count - static_cast<POS_TYPE>(1))];
}

template <typename KeyType> void DataPage<KeyType>::clearRecord(POS_TYPE pos) {
  records[static_cast<unsigned long>(pos)] = ISAMRecord<KeyType>{-1};
  count = count - static_cast<POS_TYPE>(1);
}

template <typename KeyType>
void DataPage<KeyType>::insertRecord(ISAMRecord<KeyType> record) {
  if (isFull()) {
    throw std::runtime_error("Cannot insert Record because page is full");
  } else if (getCount() == 0) {
    setRecord(record, 0);
  } else {
    auto j = static_cast<POS_TYPE>(getCount());
    while (j > 0 && record.key < getKey(j)) {
      j = j - static_cast<POS_TYPE>(1);
      setRecord(getRecord(j), j + static_cast<POS_TYPE>(1));
      count = count - static_cast<POS_TYPE>(1);
    }
    setRecord(record, j);
  }
}

template <typename KeyType> bool DataPage<KeyType>::isEmpty() {
  return getCount() == 0;
}

template <typename KeyType> DataPage<KeyType>::~DataPage() { return; }

template class DataPage<int>;
template class DataPage<float>;
