#ifndef AVL_INDEX_HPP
#define AVL_INDEX_HPP

#include "../index.hpp"
#include "avl_index_header.hpp"
#include "avl_index_node.hpp"
#include <filesystem>
#include <stdexcept>
#include <type_traits>

template <typename KEY_TYPE = default_data_type>
class AVLIndex : public Index::Index<KEY_TYPE> {

  mutable AVLIndexHeader header;

  std::string indexFileName;
  inline static std::fstream file;

  void initFile();

  void initIndex();

  void insert(physical_pos cPointer, AVLIndexNode<KEY_TYPE> &cNode,
              AVLIndexNode<KEY_TYPE> &iNode, Index::Response &response) const;

  void balance(physical_pos nodePointer) const;

  void leftRotation(physical_pos nodePointer) const;

  void rightRotation(physical_pos nodePointer) const;

  bool isBalanced(physical_pos nodePointer) const;

  int balancingFactor(physical_pos nodePointer) const;

  void updateHeigth(physical_pos nodePointer) const;

  long height(physical_pos nodePointer) const;

  AVLIndexNode<KEY_TYPE> search(physical_pos currentPointer,
                                AVLIndexNode<KEY_TYPE> &cNode,
                                Data<KEY_TYPE> &item) const;

  physical_pos maxNode(physical_pos nodePointer) const;

  bool erase(physical_pos cPointer, physical_pos pPointer,
             AVLIndexNode<KEY_TYPE> &cNode, Data<KEY_TYPE> item,
             Index::Response &response) const;

  void fixValue(physical_pos cPointer, AVLIndexNode<KEY_TYPE> &cNode,
                Data<KEY_TYPE> &item1, AVLIndexNode<KEY_TYPE> &tempNode) const;

  void displayPretty(const std::string &prefix, physical_pos cPointer,
                     bool isLeft) const;

  void rangeSearch(physical_pos cPointer, AVLIndexNode<KEY_TYPE> &cNode,
                   Index::Response &response, Data<KEY_TYPE> &begin,
                   Data<KEY_TYPE> &end) const;

public:
  using MIN_BULK_INSERT_SIZE = std::integral_constant<size_t, 0>;

  AVLIndex(std::string _indexFileName) {
    this->indexFileName = _indexFileName;
    initIndex();
  }

  AVLIndex(std::string _table_name, std::string _attribute_name,
           bool PK = false) {
    this->table_name = _table_name;
    this->attribute_name = _attribute_name;
    this->index_name = "AVL";

    std::string _directory = this->directory + "/" + this->index_name + "/" +
                             this->table_name + "/" + this->attribute_name;

    this->indexFileName = _directory + "/" + _table_name + "_" +
                          _attribute_name + "_" + this->index_name + ".bin";
    this->duplicatesFilename = _directory + "/" + _table_name + "_" +
                               _attribute_name + "_" + this->index_name +
                               "_duplicateFile.bin";
    this->PK = PK;

    if (!std::filesystem::is_directory(_directory)) {
      std::filesystem::create_directories(_directory);
    }

    initIndex();
  }

  std::string get_index_name() const override;

  Index::Response add(Data<KEY_TYPE> data, physical_pos raw_pos) const override;

  Index::Response search(Data<KEY_TYPE> item) const override;

  Index::Response remove(Data<KEY_TYPE> item) const override;

  Index::Response range_search(Data<KEY_TYPE> start,
                               Data<KEY_TYPE> end) const override;

  void displayPretty() const;

  std::pair<Index::Response, std::vector<bool>> bulk_insert(
      const std::vector<std::pair<Data<KEY_TYPE>, physical_pos>> &records)
      const override;

  void printDuplicateFile() {
    this->template printFile<AVLIndexNode<KEY_TYPE>>(this->duplicatesFilename);
  }

  void printIndexFile() {
    this->template printFileWithHeader<AVLIndexHeader, AVLIndexNode<KEY_TYPE>>(
        this->indexFileName);
  }
};

#include "avl_index.tpp"

#endif // AVL_INDEX_HPP
