#ifndef AVL_INDEX_HEADER_HPP
#define AVL_INDEX_HEADER_HPP

#include "avl_index_utils.hpp"

struct AVLIndexHeader {
  physical_pos rootPointer;
  physical_pos lastDelete;

  friend std::ostream &operator<<(std::ostream &stream,
                                  const AVLIndexHeader &header) {
    stream << " | rootPointer: " << header.rootPointer
           << " | lastDelete: " << header.lastDelete;
    return stream;
  }
};

#endif // AVL_INDEX_HEADER_HPP
