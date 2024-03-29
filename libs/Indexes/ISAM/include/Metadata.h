//
// Created by renat on 9/10/2023.
//

#ifndef INDEXED_SEQUENTIAL_ACCESS_METHOD_METADATA_H
#define INDEXED_SEQUENTIAL_ACCESS_METHOD_METADATA_H

#include "../utils/buffer.h"
#include "IndexPage.h"
#include <cstdint>
#include <fstream>
#include <string_view>

using POS_TYPE = std::streampos;
constexpr std::string_view METADATA_FILENAME = "ISAM.metadata";

/*
 * ISAM's Metadata
 * Global configuration
 */
template <typename KeyType> class Metadata {
private:
  POS_TYPE index_page_capacity;
  POS_TYPE data_page_capacity;
  POS_TYPE root_position;

public:
  Metadata();

  void setIndexPageCapacity(POS_TYPE indexPageCapacity);

  void setDataPageCapacity(POS_TYPE dataPageCapacity);

  void setRootPosition(POS_TYPE rootPosition);

  POS_TYPE getIndexPageCapacity() const;

  POS_TYPE getDataPageCapacity() const;

  POS_TYPE getRootPosition() const;

private:
  void write();

  void read();
};
extern template class Metadata<int>;
extern template class Metadata<float>;

#endif // INDEXED_SEQUENTIAL_ACCESS_METHOD_METADATA_H
