//
// Created by renat on 9/7/2023.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_INDEXPAGE_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_INDEXPAGE_H

#include "DataPage.h"
#include <fstream>
#include <sstream>

template <typename KeyType> class IndexPage {
private:
  KeyType *keys = nullptr;
  POS_TYPE *children = nullptr;
  bool isLeaf = false;

public:
  IndexPage();

  void setKey(KeyType key, int64_t pos);

  void setChild(POS_TYPE child, int64_t pos);

  void setIsLeaf(bool leaf);

  POS_TYPE write(std::fstream &file);

  void read(std::fstream &file);

  POS_TYPE size_of();

  KeyType *getKeys() const;

  POS_TYPE *getChildren() const;

  POS_TYPE getChild(POS_TYPE pos);

  KeyType getkey(POS_TYPE pos);

  bool getIsLeaf() const;

  POS_TYPE getCapacity() const;

  ~IndexPage();
};

extern template class IndexPage<int>;
extern template class IndexPage<float>;

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_INDEXPAGE_H
