//
// Created by renat on 9/7/2023.
//

#include "../include/IndexPage.h"

template <typename KeyType> IndexPage<KeyType>::IndexPage() {
  int64_t capacity = getCapacity();
  keys = new KeyType[static_cast<unsigned long>(capacity)];
  children = new POS_TYPE[static_cast<unsigned long>(capacity + 1)];
}

template <typename KeyType>
void IndexPage<KeyType>::setKey(KeyType key, int64_t pos) {
  try {
    keys[pos] = key;
  } catch (std::exception &e) {
    throw(std::runtime_error("Cannot read key: " + std::to_string(key) +
                             " in position: " + std::to_string(pos)));
  }
}

template <typename KeyType>
void IndexPage<KeyType>::setChild(POS_TYPE child, int64_t pos) {
  try {
    children[pos] = child;
  } catch (std::exception &e) {
    throw(std::runtime_error("Cannot read child: " + std::to_string(child) +
                             " in position: " + std::to_string(pos)));
  }
}

template <typename KeyType> void IndexPage<KeyType>::setIsLeaf(bool leaf) {
  this->isLeaf = leaf;
}

template <typename KeyType>
POS_TYPE IndexPage<KeyType>::write(std::fstream &file) {
  POS_TYPE pos = file.tellp();
  int64_t capacity = this->getCapacity();
  // Write isLeaf attribute
  file.write((char *)&isLeaf, sizeof(isLeaf));
  // Write keys
  for (int i = 0; i < capacity; ++i) {
    file.write((char *)&(keys[i]), sizeof(keys[i]));
  }
  // Write children positions
  for (int i = 0; i < capacity + 1; ++i) {
    file.write((char *)&(children[i]), sizeof(children[i]));
  }
  return pos;
}

template <typename KeyType> void IndexPage<KeyType>::read(std::fstream &file) {
  int64_t capacity = this->getCapacity();
  // Read isLeaf attribute
  file.read((char *)&isLeaf, sizeof(isLeaf));
  // Read keys
  for (int i = 0; i < capacity; ++i) {
    file.read((char *)&keys[i], sizeof(keys[i]));
  }
  // Read children positions
  for (int i = 0; i < capacity + 1; ++i) {
    file.read((char *)&children[i], sizeof(children[i]));
  }
}

template <typename KeyType> POS_TYPE IndexPage<KeyType>::size_of() {
  int64_t capacity = this->getCapacity();
  return static_cast<POS_TYPE>(
      sizeof(KeyType) * static_cast<unsigned long>(capacity) +
      sizeof(POS_TYPE) * (static_cast<unsigned long>(capacity + 1)) +
      sizeof(bool));
}

template <typename KeyType> KeyType *IndexPage<KeyType>::getKeys() const {
  return keys;
}

template <typename KeyType> POS_TYPE *IndexPage<KeyType>::getChildren() const {
  return children;
}

template <typename KeyType> bool IndexPage<KeyType>::getIsLeaf() const {
  return isLeaf;
}

template <typename KeyType>
POS_TYPE IndexPage<KeyType>::getChild(POS_TYPE pos) {
  return children[pos];
}

template <typename KeyType> KeyType IndexPage<KeyType>::getkey(POS_TYPE pos) {
  return keys[pos];
}

/*
 * M: Number of keys of ISAM Indexes Pages
 *              |------------------keys-----------------|
 * |------------------children---------------|  |---- isLeaf ---| BufferSize =
 * sizeof(KeyType*) + (sizeof(KeyType) * M) + sizeof(KeyType*) +
 * sizeof(POS_TYPE) * (M+1) + sizeof(bool) BufferSize = sizeof(KeyType*) +
 * (sizeof(KeyType) * M) + sizeof(KeyType*) + (sizeof(POS_TYPE) * M) +
 * sizeof(POS_TYPE) + sizeof(bool) BufferSize = M * (sizeof(KeyType) +
 * sizeof(POS_TYPE)) + sizeof(POS_TYPE) + (2 * sizeof(KeyType*)) + sizeof(bool)
 * M = (BufferSize - sizeof(POS_TYPE) - (2 * sizeof(KeyType*)) - sizeof(bool)) /
 * (sizeof(KeyType) + sizeof(POS_TYPE))
 *
 */

template <typename KeyType> POS_TYPE IndexPage<KeyType>::getCapacity() const {
  //  auto a = std::floor((buffer::get_buffer_size() - sizeof(POS_TYPE) -
  //                       2 * sizeof(KeyType *) - sizeof(bool)) /
  //                      (sizeof(KeyType) + sizeof(POS_TYPE)));
  return POS_TYPE(2);
}

template <typename KeyType> IndexPage<KeyType>::~IndexPage() {
  delete[] keys;
  delete[] children;
}
template class IndexPage<int>;
template class IndexPage<float>;
