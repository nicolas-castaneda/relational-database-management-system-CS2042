#ifndef AVL_INDEX_NODE_HPP
#define AVL_INDEX_NODE_HPP

#include "../index.hpp"
#include "record.hpp"

template <typename KEY_TYPE> struct AVLIndexNode : public Index::Record<KEY_TYPE> {

  physical_pos current_pos{};

  physical_pos leftChildren = -1;
  physical_pos rightChildren = -1;

  physical_pos nextDelete = -1;

  physical_pos height = 0;

  friend std::ostream &operator<<(std::ostream &os, const AVLIndexNode &node) {

    os << " | Data: " << node.data;
    os << " | raw_pos:" << node.raw_pos;
    os << " | Left Children: " << node.leftChildren;
    os << " | Right Children: " << node.rightChildren;
    os << " | Next Delete: " << node.nextDelete;
    os << " | Height: " << node.height;
    return os;
  }
};

#endif // AVL_INDEX_NODE_HPP
